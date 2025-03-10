db.messages.aggregate([
  {
    $match: {
      $and: [
        { behaviors: { $exists: true } },
        { behaviors: { $not: { $size: 0 } } }
      ]
    }
  },
  {
    $lookup: {
      from: "campaigns",
      localField: "campaign_id",
      foreignField: "id",
      as: "campaign",
      let: { message_type: "$message_type" },
      pipeline: [
        {
          $match: {
            $expr: { $eq: [ "$campaign_type",  "$$message_type" ] }
          }
        }
      ]
    }
  },
  {
    $unwind: "$campaign"
  },
  {
    $addFields: {
      "user_id": { $toInt: { $substr: [ { $toString: "$client_id" }, 9, 9 ] } },
      "interactions": {
        $filter: {
          input: "$behaviors",
          as: "behavior",
          cond: { $in: ["$$behavior.type", ["clicked", "opened", "purchased"]] }
        }
      },
      has_interaction: { 
        $gt: [
          {
            $size: {
              $filter: {
                input: "$behaviors",
                as: "behavior",
                cond: { $in: ["$$behavior.type", ["clicked", "opened"]] }
              }
            }
          },
          0
        ]
      }
    }
  },
  {
    $match: {
      "interactions.type": "purchased"
    }
  },
  {
    $lookup: {
      from: "friends",
      let: { user_id: "$user_id" },
      pipeline: [
        {
          $match: {
            $expr: {
              $or: [
                { $eq: ["$friend1", "$$user_id"] },
                { $eq: ["$friend2", "$$user_id"] }
              ]
            }
          }
        },
        {
          $project: {
            _id: 0,
            friend: {
              $cond: {
                if: { $eq: ["$friend1", "$$user_id"] },
                then: "$friend2",
                else: "$friend1"
              }
            }
          }
        }
      ],
      as: "friends"
    }
  },
  {
    $unwind: "$friends"
  },
  {
    $lookup: {
      from: "messages",
      let: {
        campaign_id: "$campaign_id",
        campaign_type: "$campaign.campaign_type",
        friend_user_id: "$friends.friend"
      },
      pipeline: [
        {
          $match: {
            $expr: {
              $and: [
                { $eq: ["$campaign_id", "$$campaign_id"] },
                { $eq: ["$message_type", "$$campaign_type"] },
                { $eq: [{$toInt: {$substr:[{$toString: "$client_id"}, 9, 9]}}, "$$friend_user_id"] },
                {
                  $in: [
                    "purchased",
                    {
                      $map: {
                        input: "$behaviors",
                        as: "behavior",
                        in: "$$behavior.type"
                      }
                    }
                  ]
                }
              ]
            }
          }
        },
        {
          $project: {
            _id: 0,
            "user_id": { $toInt: { $substr: [ { $toString: "$client_id" }, 9, 9 ] } }
          }
        }
      ],
      as: "friendPurchaseMessage"
    }
  },
  {
    $unwind: { path: "$friendPurchaseMessage", preserveNullAndEmptyArrays: true }
  },
  {
    $group: { 
      _id: {
        original_message_id: "$_id",
        campaign_id: "$campaign_id",
        campaign_type: "$campaign.campaign_type",
        client_id: "$client_id",
        user_id: "$user_id",
        has_interaction: "$has_interaction"
      },
      friend_user_id: { $addToSet: "$friendPurchaseMessage.user_id" } 
    }
  },
{
    $addFields:{
      "friend_user_id": { $setDifference: ["$friend_user_id", [null]] }
    }
  },
   {
    $match:{
       "friend_user_id.0": {$exists: true}
    }
  },
  {
    $group: {
      _id: { campaign_id: "$_id.campaign_id", campaign_type: "$_id.campaign_type" },
      total_messages: { $addToSet: "$_id.client_id" },
      clients_with_interaction: {
        $addToSet: {
          $cond: {
            if: "$_id.has_interaction", 
            then: "$_id.client_id",
            else: null
          }
        }
      },
      users_purchased: { $addToSet: "$_id.user_id" },
      friends_who_also_purchased: { $first: "$friend_user_id" } 
    }
  },
  {
    $project: {
      _id: 0,
      campaign_id: "$_id.campaign_id",
      campaign_type: "$_id.campaign_type",
      total_messages: { $size: "$total_messages" },
      clients_with_interaction: { $size: "$clients_with_interaction" },
      users_purchased: { $size: "$users_purchased" },
      friends_who_also_purchased: { $size: "$friends_who_also_purchased" },
      conversion_rate: {
        $cond: {
          if: { $gt: [{ $size: "$total_messages" }, 0] },
          then: {
            $multiply: [
              { $divide: [{ $size: "$users_purchased" }, { $size: "$total_messages" }] },
              100
            ]
          },
          else: 0
        }
      }
    }
  },
  {
    $sort: { conversion_rate: -1 }
  },
  {
    $limit: 10
  }
])