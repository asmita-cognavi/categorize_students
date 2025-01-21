import pymongo

# MongoDB connection
client = pymongo.MongoClient("mongodb+srv://dev-student:25HHL5dwb6MQlFbK@student.qwtbgls.mongodb.net/?retryWrites=true&w=majority")
db = client["DEV_STUDENT"]
student_skills_collection = db["student_skills_temp"]
students_collection = db["students"]

pipeline = [
    {
        "$addFields": {
            "string_id": { "$toString": "$_id" }
        }
    },
    {
        "$lookup": {
            "from": "student_skills_temp",
            "let": { "student_string_id": "$string_id" },
            "pipeline": [
                {
                    "$match": {
                        "$expr": { "$eq": ["$student_id", "$$student_string_id"] }
                    }
                }
            ],
            "as": "skills_match"
        }
    },
    {
        "$project": {
            "_id": 1,
            "full_name": {
                "$concat": [
                    {"$ifNull": ["$first_name", ""]},
                    " ",
                    {"$ifNull": ["$last_name", ""]}
                ]
            },
            "work_experience_count": {"$size": {"$ifNull": ["$work_experiences", []]}},
            "project_count": {"$size": {"$ifNull": ["$projects", []]}},
            "skills_data": { "$arrayElemAt": ["$skills_match.student_skills", 0] },
            "achievement_count": {
                "$add": [
                    {"$size": {"$ifNull": ["$awards", []]}},
                    {"$size": {"$ifNull": ["$achievements", []]}}
                ]
            },
            "source_bonus": {
                "$cond": [
                    {"$eq": ["$source", "coresignal"]},
                    10,
                    0
                ]
            },
            "cgpa": {
                "$let": {
                    "vars": {
                        "primary_edu": {
                            "$arrayElemAt": [
                                {
                                    "$filter": {
                                        "input": {"$ifNull": ["$education_records", []]},
                                        "as": "edu",
                                        "cond": {"$eq": ["$$edu.is_primary", True]}
                                    }
                                },
                                0
                            ]
                        }
                    },
                    "in": {
                        "$switch": {
                            "branches": [
                                {
                                    "case": {"$eq": ["$$primary_edu.performance_scale", "cgpa"]},
                                    "then": "$$primary_edu.performance"
                                },
                                {
                                    "case": {"$eq": ["$$primary_edu.performance_scale", "percentage"]},
                                    "then": {"$divide": ["$$primary_edu.performance", 10]}
                                }
                            ],
                            "default": 0
                        }
                    }
                }
            }
        }
    },
    {
        "$addFields": {
            "hard_skill_count": {
                "$size": {
                    "$filter": {
                        "input": { "$ifNull": ["$skills_data", []] },
                        "cond": { "$eq": ["$$this.skill_type", "Hard Skill"] }
                    }
                }
            },
            "soft_skill_count": {
                "$size": {
                    "$filter": {
                        "input": { "$ifNull": ["$skills_data", []] },
                        "cond": { "$eq": ["$$this.skill_type", "Soft Skill"] }
                    }
                }
            }
        }
    },
    {
        "$addFields": {
            "total_count": {
                "$add": [
                    "$work_experience_count",
                    "$project_count",
                    { "$add": ["$hard_skill_count", "$soft_skill_count"] },
                    "$achievement_count"
                ]
            }
        }
    }
]

try:
    result = students_collection.aggregate(pipeline)
    
    # Print header
    print("\nStudent Statistics:")
    print("-" * 160)
    print(f"{'ID':^24} {'Name':<30} {'Work Exp':>10} {'Projects':>10} {'Hard Skills':>12} {'Soft Skills':>12} "
          f"{'Achieve':>10} {'Bonus':>8} {'CGPA':>8} {'Total':>10}")
    print("-" * 160)
    
    # Display the results
    for record in result:
        cgpa_display = f"{float(record['cgpa']):.2f}"
        
        print(f"{str(record['_id']):^24} {record['full_name']:<30} "
              f"{record['work_experience_count']:>10} {record['project_count']:>10} "
              f"{record['hard_skill_count']:>12} {record['soft_skill_count']:>12} "
              f"{record['achievement_count']:>10} {record['source_bonus']:>8} "
              f"{cgpa_display:>8} {record['total_count']:>10}")

except Exception as e:
    print(f"Error: {e}")

finally:
    client.close()
    