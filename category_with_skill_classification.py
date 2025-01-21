import pymongo
from typing import List, Dict
from openai import OpenAI

# MongoDB connection
client = pymongo.MongoClient("mongodb+srv://dev-student:25HHL5dwb6MQlFbK@student.qwtbgls.mongodb.net/?retryWrites=true&w=majority")
db = client["DEV_STUDENT"]
collection = db["students"]

# Initialize OpenAI client
llm_client = OpenAI(api_key='key')

def classify_skills(skill_names: List[str]) -> Dict[str, List[str]]:
    """
    Classify skills as hard or soft using LLM
    """
    # Join skills into a single string
    skills_text = ", ".join(skill_names)
    
    try:
        response = llm_client.chat.completions.create(
            model="gpt-4o-mini",
            messages=[
                {
                    "role": "system",
                    "content": """You are a skill classifier. For each skill provided, classify it as either 'hard' or 'soft'. Ensure all skills are classified.
                    Example classifications:
                    - Programming: hard
                    - Leadership: soft
                    - Python: hard
                    - Communication: soft
                    - Database Management: hard
                    - Teamwork: soft
                    
                    Return only two lines starting with 'hard:' and 'soft:' followed by the skills in each category. Strictly follow the return pattern and make sure all skills are classified.
                    Example:
                    
                    hard:agile methodology,programming,sql
                    soft:problem solving,teamwork
                    """
                },
                {
                    "role": "user",
                    "content": f"Classify these skills: {skills_text}"
                }
            ],
            temperature=0.2,
            max_tokens=256,
            top_p=0.5
        )

        # Extract the content from the response
        response_text = response.choices[0].message.content
        classified_skills = {"hard_skills": [], "soft_skills": []}
        
        for line in response_text.split('\n'):
            line = line.strip()
            if line.startswith('hard:'):
                skills_list = line[5:].strip().split(',')
                classified_skills['hard_skills'] = [s.strip() for s in skills_list if s.strip() in skill_names]
            elif line.startswith('soft:'):
                skills_list = line[5:].strip().split(',')
                classified_skills['soft_skills'] = [s.strip() for s in skills_list if s.strip() in skill_names]
        
        return classified_skills
    except Exception as e:
        print(f"Error in skill classification: {e}")
        return {"hard_skills": [], "soft_skills": []}



def calculate_score(project_count: int, hard_skills_count: int, work_exp_count: int, 
                   achievement_count: int, soft_skills_count: int, cgpa: float, 
                   source_bonus: int) -> Dict[str, float]:
    """
    Calculate student score based on various criteria
    Returns a dictionary with base scores (out of 100) and bonus scores (out of 30)
    """
    scores = {}
    bonus_scores = {}
    category=""
    
    # 1. Projects (25%)
    if project_count >= 2:
        scores['project_score'] = 25
        bonus_scores['project_bonus'] = 10 if project_count > 2 else 0
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
        bonus_scores['work_exp_bonus'] = 10 if work_exp_count > 2 else 0
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
    
    # Source bonus (from coresignal)
    bonus_scores['source_bonus'] = source_bonus
    
    # Calculate totals
    scores['base_total'] = sum(scores.values())
    bonus_scores['bonus_total'] = sum(bonus_scores.values())
    total_score=scores['base_total'] + bonus_scores['bonus_total']
    if(total_score>=100):
        category="C1"
    elif (total_score>=80 and total_score<=99):
        category="C2"
    elif (total_score>=60 and total_score<=79):
        category="C3"
    elif (total_score>=42 and total_score<=59): #42 is the minimum score that indicates all fields are present
        category="C4"
    elif (total_score<42):
        category="C5"
    return {
        'base_scores': scores,
        'bonus_scores': bonus_scores,
        'total_score': total_score,
        'category':category
    }
    
    
    
    
pipeline = [
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
            "skills": {"$ifNull": ["$skills", []]},  # Keep the full skills array
            "work_experience_count": {"$size": {"$ifNull": ["$work_experiences", []]}},
            "project_count": {"$size": {"$ifNull": ["$projects", []]}},
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
    }
]




try:
    result = list(collection.aggregate(pipeline))
    
    # Print header
    print("\nStudent Statistics and Scores:")
    print("-" * 200)
    print(f"{'ID':^24} {'Name':<30} {'Work Exp':>10} {'Projects':>10} {'Hard Skills':>12} "
          f"{'Soft Skills':>12} {'Achieve':>10} {'CGPA':>8} {'Base(/100)':>12} {'Bonus(/30)':>12} {'Total':>12} {'Category':>10}")
    print("-" * 200)
    c1=0
    c2=0
    c3=0
    c4=0
    c5=0
    # Process and display the results
    for record in result:
        # Extract skill names and classify
        if record.get('skills'):
            skill_names = [skill['name'] for skill in record['skills'] if 'name' in skill]
            classified_skills = classify_skills(skill_names)
            hard_skills_count = len(classified_skills['hard_skills'])
            soft_skills_count = len(classified_skills['soft_skills'])
        else:
            hard_skills_count = 0
            soft_skills_count = 0
            
        cgpa = float(record['cgpa'])
        
        # Calculate scores
        scores = calculate_score(
            project_count=record['project_count'],
            hard_skills_count=hard_skills_count,
            work_exp_count=record['work_experience_count'],
            achievement_count=record['achievement_count'],
            soft_skills_count=soft_skills_count,
            cgpa=cgpa,
            source_bonus=record['source_bonus']
        )
        
        print(f"{str(record['_id']):^24} {record['full_name']:<30} "
              f"{record['work_experience_count']:>10} {record['project_count']:>10} "
              f"{hard_skills_count:>12} {soft_skills_count:>12} "
              f"{record['achievement_count']:>10} {cgpa:>8.2f} "
              f"{scores['base_scores']['base_total']:>12} "
              f"{scores['bonus_scores']['bonus_total']:>12} "
              f"{scores['total_score']:>12}"
              f"{scores['category']:>10}")
        if(scores['category']=='C1'):
            c1+=1
        elif(scores['category']=='C2'):
            c2+=1
        elif(scores['category']=='C3'):
            c3+=1
        elif(scores['category']=='C4'):
            c4+=1
        elif(scores['category']=='C5'):
            c5+=1
    print(c1)
    print(c2)
    print(c3)
    print(c4)
    print(c5)
except Exception as e:
    print(f"Error in main execution: {e}")
finally:
    client.close()