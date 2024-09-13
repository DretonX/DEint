# 1. Top 5 most frequent comments
db.processed_data.aggregate([
  { $group: { _id: "$content", count: { $sum: 1 } } },
  { $sort: { count: -1 } },
  { $limit: 5 }
])

# 2. All records where the length of the "content" field is less than 5 characters
db.processed_data.aggregate([
  { $project: { content: 1, contentLength: { $strLenCP: "$content" } } },
  { $match: { contentLength: { $lt: 5 } } }
])

# 3. Average rating for each day
db.processed_data.aggregate([
  { $group: {
      _id: { $dateToString: { format: "%Y-%m-%d", date: "$at" } },
      averageRating: { $avg: "$score" }
    }
  },
  { $project: {
      _id: 0,
      date: { $toDate: "$_id" },
      averageRating: 1
    }
  },
  { $sort: { date: 1 } }
])