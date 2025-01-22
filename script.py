import pymongo
from typing import Dict, List
from multiprocessing import Pool, cpu_count
import pandas as pd
from functools import partial
import time
from datetime import datetime
import logging

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

def calculate_score(project_count: int, hard_skills_count: int, work_exp_count: int,
                   achievement_count: int, soft_skills_count: int, cgpa: float,
                   source_bonus: int) -> Dict[str, float]:
    """
    Calculate student score based on various criteria
    Returns a dictionary with base scores (out of 100) and bonus scores (out of 30)
    """
    scores = {}
    bonus_scores = {}
    
    # 1. Projects (25%)
    if project_count >= 2:
        scores['project_score'] = 25
        bonus_scores['project_bonus'] = 5 if project_count > 2 else 0
    elif project_count == 1:
        scores['project_score'] = 15
        bonus_scores['project_bonus'] = 0
    else:
        scores['project_score'] = 0
        bonus_scores['project_bonus'] = 0
    
    # 2. Hard Skills (20%)
    if hard_skills_count >= 5:
        scores['hard_skills_score'] = 20
    elif hard_skills_count >= 3:
        scores['hard_skills_score'] = 10
    elif hard_skills_count >= 1:
        scores['hard_skills_score'] = 5
    else:
        scores['hard_skills_score'] = 0
    
    # 3. Work Experience (20%)
    if work_exp_count >= 2:
        scores['work_exp_score'] = 20
        bonus_scores['work_exp_bonus'] = 5 if work_exp_count > 2 else 0
    elif work_exp_count == 1:
        scores['work_exp_score'] = 15
        bonus_scores['work_exp_bonus'] = 0
    else:
        scores['work_exp_score'] = 0
        bonus_scores['work_exp_bonus'] = 0
    
    # 4. Achievements (15%)
    if achievement_count >= 3:
        scores['achievement_score'] = 15
    elif achievement_count == 2:
        scores['achievement_score'] = 10
    elif achievement_count == 1:
        scores['achievement_score'] = 5
    else:
        scores['achievement_score'] = 0
    
    # 5. Soft Skills (10%)
    if soft_skills_count >= 4:
        scores['soft_skills_score'] = 10
    elif soft_skills_count == 3:
        scores['soft_skills_score'] = 8
    elif soft_skills_count == 2:
        scores['soft_skills_score'] = 5
    elif soft_skills_count == 1:
        scores['soft_skills_score'] = 3
    else:
        scores['soft_skills_score'] = 0
    
    # 6. CGPA (10%)
    if cgpa > 8:
        scores['cgpa_score'] = 10
    elif cgpa >= 7:
        scores['cgpa_score'] = 7
    elif cgpa >= 6:
        scores['cgpa_score'] = 5
    elif cgpa > 0:
        scores['cgpa_score'] = 2
    else:
        scores['cgpa_score'] = 0
   
    # Source bonus
    bonus_scores['source_bonus'] = source_bonus
   
    # Calculate totals
    scores['base_total'] = sum(scores.values())
    bonus_scores['bonus_total'] = sum(bonus_scores.values())
    total_score = scores['base_total'] + bonus_scores['bonus_total']
    percentage=round((total_score/115)*100)
    # Determine category
    if total_score >= 100:
        category = "C1"
    elif 80 <= total_score <= 99:
        category = "C2"
    elif 60 <= total_score <= 79:
        category = "C3"
    elif 42 <= total_score <= 59:
        category = "C4"
    else:
        category = "C5"
   
    return {
        'base_scores': scores,
        'bonus_scores': bonus_scores,
        'total_score': total_score,
        'category': category,
        'percentage':percentage
    }

def process_student_chunk(students: List[Dict]) -> List[Dict]:
    """Process a chunk of students and calculate their scores"""
    results = []
    for student in students:
        try:
            scores = calculate_score(
                student['project_count'],
                student['hard_skill_count'],
                student['work_experience_count'],
                student['achievement_count'],
                student['soft_skill_count'],
                student['cgpa'],
                student['source_bonus']
            )
          
            results.append({
                'student_id': student['_id'],
                'full_name': student['full_name'],
                'metrics': {
                    'work_experience_count': student['work_experience_count'],
                    'project_count': student['project_count'],
                    'hard_skill_count': student['hard_skill_count'],
                    'soft_skill_count': student['soft_skill_count'],
                    'achievement_count': student['achievement_count'],
                    'source_bonus': student['source_bonus'],
                    'cgpa': student['cgpa']
                },
                'scores': scores
            })
        except Exception as e:
            logger.error(f"Error processing student {student.get('_id', 'unknown')}: {str(e)}")
   
    return results

def get_pipeline(batch_size: int, skip: int = 0):
    """Get MongoDB aggregation pipeline with pagination"""
    return [
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
                        5,
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
        { "$skip": skip },
        { "$limit": batch_size }
    ]

def main():
    client = pymongo.MongoClient(
        "mongodb+srv://dev-student:25HHL5dwb6MQlFbK@student.qwtbgls.mongodb.net/?retryWrites=true&w=majority",
        serverSelectionTimeoutMS=10000,
        maxPoolSize=None
    )
    db = client["DEV_STUDENT"]
    
    try:
        batch_size = 5000  
        skip = 0
        all_results = []
        
        num_cores = cpu_count()
        pool = Pool(processes=num_cores)
        
        while True:
            try:
                logger.info(f"Processing batch starting at offset {skip}")
                
                pipeline = get_pipeline(batch_size, skip)
                batch = list(db.students.aggregate(pipeline, allowDiskUse=True))
                
                if not batch:
                    break
                
                chunk_size = max(1, len(batch) // num_cores)
                chunks = [batch[i:i + chunk_size] for i in range(0, len(batch), chunk_size)]
                
                chunk_results = pool.map(process_student_chunk, chunks)
                
                for chunk_result in chunk_results:
                    all_results.extend(chunk_result)
                
                skip += batch_size
                
            except pymongo.errors.ExecutionTimeout as e:
                logger.warning(f"Timeout encountered at offset {skip}. Reducing batch size and retrying...")
                batch_size = max(100, batch_size // 2)
                continue
                
            except Exception as e:
                logger.error(f"Error processing batch at offset {skip}: {str(e)}")
                break
        
        pool.close()
        pool.join()
        
        df = pd.DataFrame(all_results)
        
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        output_file = f'student_scores_{timestamp}.csv'
        df.to_csv(output_file, index=False)
        
        print("\nCategory Distribution:")
        print("-" * 40)
        category_counts = df['scores'].apply(lambda x: x['category']).value_counts().sort_index()
        for category, count in category_counts.items():
            print(f"Category {category}: {count} students")
        print(f"\nTotal Students Processed: {len(df)}")
        print(f"\nResults saved to: {output_file}")
        
    except Exception as e:
        logger.error(f"Main process error: {str(e)}")
    
    finally:
        client.close()

if __name__ == '__main__':
    main()
