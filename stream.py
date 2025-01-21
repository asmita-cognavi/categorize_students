import streamlit as st
import pandas as pd
import pymongo
from typing import Dict, List
from multiprocessing import Pool, cpu_count
import plotly.express as px
import plotly.graph_objects as go
from datetime import datetime
import os
import glob
import time
import logging
from pathlib import Path

# Setup logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Initialize session state variables
if 'processing_complete' not in st.session_state:
    st.session_state.processing_complete = False
if 'current_category' not in st.session_state:
    st.session_state.current_category = None
if 'total_students' not in st.session_state:
    st.session_state.total_students = 0
if 'avg_score' not in st.session_state:
    st.session_state.avg_score = 0
if 'top_category' not in st.session_state:
    st.session_state.top_category = None

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
    
    # Source bonus
    bonus_scores['source_bonus'] = source_bonus
    
    # Calculate totals
    scores['base_total'] = sum(scores.values())
    bonus_scores['bonus_total'] = sum(bonus_scores.values())
    total_score = scores['base_total'] + bonus_scores['bonus_total']
    
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
        'category': category
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
        { "$skip": skip },
        { "$limit": batch_size }
    ]


def load_existing_data():
    """Load the most recent CSV file if it exists"""
    # First try exact file if known
    if os.path.exists('student_scores_20250121_203853.csv'):
        df = pd.read_csv('student_scores_20250121_203853.csv')
        # Convert string representations of dictionaries to actual dictionaries
        df['metrics'] = df['metrics'].apply(eval)
        df['scores'] = df['scores'].apply(eval)
        return df
    
    # Fallback to searching for any student_scores files
    csv_files = glob.glob('student_scores_*.csv')
    if not csv_files:
        return None
    
    latest_file = max(csv_files, key=os.path.getctime)
    df = pd.read_csv(latest_file)
    df['metrics'] = df['metrics'].apply(eval)
    df['scores'] = df['scores'].apply(eval)
    return df

def process_new_students(existing_df):
    """Process only new students and append their results to the existing DataFrame."""
    if existing_df is None:
        raise ValueError("An existing DataFrame must be provided.")

    # Convert existing student IDs to strings for comparison
    existing_ids = set(existing_df['student_id'].astype(str))

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
        new_students_found = False

        progress_bar = st.progress(0)
        status_text = st.empty()

        num_cores = cpu_count()
        pool = Pool(processes=num_cores)

        while True:
            try:
                status_text.text(f"Processing batch starting at offset {skip}")

                pipeline = get_pipeline(batch_size, skip)
                batch = list(db.students.aggregate(pipeline, allowDiskUse=True))

                if not batch:
                    break

                # Filter out existing students
                new_batch = [student for student in batch 
                           if str(student['_id']) not in existing_ids]

                if new_batch:
                    new_students_found = True
                    chunk_size = max(1, len(new_batch) // num_cores)
                    chunks = [new_batch[i:i + chunk_size] for i in range(0, len(new_batch), chunk_size)]

                    chunk_results = pool.map(process_student_chunk, chunks)

                    for chunk_result in chunk_results:
                        all_results.extend(chunk_result)

                skip += batch_size
                progress_bar.progress(min(1.0, len(all_results) / 10000))  # Assuming total

            except Exception as e:
                logger.error(f"Error processing batch at offset {skip}: {str(e)}")
                break

        pool.close()
        pool.join()

        if new_students_found and all_results:
            # Append new results to the existing DataFrame
            df_new = pd.DataFrame(all_results)
            existing_df = pd.concat([existing_df, df_new], ignore_index=True)
            
            # Save the updated DataFrame
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            output_file = f'student_scores_{timestamp}.csv'
            existing_df.to_csv(output_file, index=False)

        else:
            st.info("No new students to process.")

        return existing_df

    finally:
        client.close()

def create_category_metrics(df):
    """Create metrics for each category"""
    category_stats = {}
    for cat in ['C1', 'C2', 'C3', 'C4', 'C5']:
        cat_df = df[df['scores'].apply(lambda x: x['category'] == cat)]
        category_stats[cat] = {
            'count': len(cat_df),
            'avg_score': cat_df['scores'].apply(lambda x: x['total_score']).mean()
        }
    return category_stats

def create_distribution_plot(df):
    """Create category distribution plot"""
    category_counts = df['scores'].apply(lambda x: x['category']).value_counts()
    
    # Create bar plot with color mapping
    fig = px.bar(
        x=category_counts.index,
        y=category_counts.values,
        title='Student Category Distribution',
        labels={'x': 'Category', 'y': 'Number of Students'},
        color=category_counts.index,  # This maps colors to categories
        color_discrete_sequence=px.colors.qualitative.Set3,  # Use Set3 color palette
    )
    
    fig.update_traces(
        hovertemplate='Number of Students: %{y}<extra></extra>'  # Only show number of students
    )
    return fig

def main():
    if 'initialized' not in st.session_state:
        st.session_state.initialized = False
        st.session_state.df = None
        st.session_state.processing_complete = False

    # Set the page configuration
    st.set_page_config(page_title="Student Analysis Dashboard", layout="wide")

    # Title
    st.title("🎓 **Student Analysis Dashboard**")
    st.markdown("<br>", unsafe_allow_html=True)
    st.markdown("<br>", unsafe_allow_html=True)

    # Main page title
    st.write("CONTROLS")

    # Create columns for side-by-side display of buttons
    col1, col2 = st.columns(2)

    # Display buttons in the first and second columns
    with col1:
        process_button = st.button("📋 Begin Display")

    with col2:
        process_button1 = st.button("🔄 Process New Students")

    # Sidebar content
    st.sidebar.title("Score Details")

    # Category based on Total Score
    # st.sidebar.subheader("Category Breakdown")
    st.sidebar.write("""
    - **C1**: Total Score ≥ 100
    - **C2**: 80 ≤ Total Score ≤ 99
    - **C3**: 60 ≤ Total Score ≤ 79
    - **C4**: 42 ≤ Total Score ≤ 59
    - **C5**: Total Score < 42
    """)

    # Secondary Score Explanation
    # st.sidebar.subheader("Secondary Score")
    st.sidebar.write("""
    The **Secondary Score**:
    - This is a count of Work Experience, Projects, Hard Skills, Soft Skills, and Achievements.
    - It is used for sorting when the Total Score is same for students.
    """)

    # Bonus Score Explanation
    # st.sidebar.subheader("Bonus Score")
    st.sidebar.write("""
    **Bonus Score (up to 30 points)** in 3 ways:
    1. If the student is from IIT or NIT.
    2. If the student has more than 2 projects.
    3. If the student has more than 2 work experiences.
    """)
    # Debugging information
    # st.sidebar.write("Session State:", st.session_state)
    # st.sidebar.write("File exists:", os.path.exists('student_scores_20250121_203853.csv'))
    # st.sidebar.write("Current working directory:", os.getcwd())

    # Initialize or load data
    if 'df' not in st.session_state or process_button or process_button1:
        with st.spinner("Beginning Process..."):
            existing_df = load_existing_data()

        if process_button:
            with st.spinner("Displaying existing data..."):
                st.session_state.df = existing_df
        else:
            st.session_state.df = existing_df
        
        if process_button1:
            with st.spinner("Processing new students..."):
                st.session_state.df = process_new_students(existing_df)
        else:
            st.session_state.df = existing_df
        
    st.markdown("<br>", unsafe_allow_html=True)


    # Main content
    if st.session_state.df is not None:
        # KPI metrics at the top
        st.subheader("📊 **Key Performance Indicators (KPIs)**")
        col1, col2, col3, col4 = st.columns(4)

        with col1:
            st.markdown("""
                <div style="border: 2px solid steelblue; padding: 5px; border-radius: 5px;">
                    <h5 style="text-align: center; color: steelblue;">Total Students</h5>
                    <h2 style="text-align: center;">{}</h2>
                </div>
            """.format(len(st.session_state.df)), unsafe_allow_html=True)

        with col2:
            avg_score = st.session_state.df['scores'].apply(lambda x: x['total_score']).mean()
            st.markdown("""
                <div style="border: 2px solid steelblue; padding: 5px;border-radius: 5px;">
                    <h5 style="text-align: center; color: steelblue;">Average Score</h5>
                    <h2 style="text-align: center;">{:.2f}</h2>
                </div>
            """.format(avg_score), unsafe_allow_html=True)

        with col3:
            top_category = st.session_state.df['scores'].apply(lambda x: x['category']).mode()[0]
            st.markdown("""
                <div style="border: 2px solid steelblue; padding: 5px; border-radius: 5px;">
                    <h5 style="text-align: center; color: steelblue;">Top Category</h5>
                    <h2 style="text-align: center;">{}</h2>
                </div>
            """.format(top_category), unsafe_allow_html=True)

        with col4:
            max_score = st.session_state.df['scores'].apply(lambda x: x['total_score']).max()
            st.markdown("""
                <div style="border: 2px solid steelblue; padding: 5px; border-radius: 5px;">
                    <h5 style="text-align: center; color: steelblue;">Highest Score</h5>
                    <h2 style="text-align: center;">{}</h2>
                </div>
            """.format(max_score), unsafe_allow_html=True)
        
        st.markdown("<br>", unsafe_allow_html=True)


        # Display distribution plot
        # if st.checkbox("📊 Show Score Distribution Plot"):

        st.subheader("📈 **Distribution Plot**")
        distribution_fig = create_distribution_plot(st.session_state.df)
        distribution_fig.update_layout(
            title="Score Distribution",
            xaxis_title="Category",
            yaxis_title="Number of Students",
            template="plotly_white",
            showlegend=True, 
            legend_title_text=None
        )
        st.plotly_chart(distribution_fig, key="unique_chart_1")
        st.markdown("<hr style='border:1px solid #ccc;'>", unsafe_allow_html=True)
    
        # Category selection
        st.subheader("📂 **Category Analysis**")
        categories = ['All'] + sorted(st.session_state.df['scores'].apply(lambda x: x['category']).unique().tolist())
        selected_category = st.selectbox("Select a Category:", categories)

    

        # Display student table
        st.subheader("📋 **Student Details**")

        filtered_df = st.session_state.df
        if selected_category != 'All':
            filtered_df = filtered_df[filtered_df['scores'].apply(lambda x: x['category'] == selected_category)]

        # Modified display data creation
        display_data = []
        for _, row in filtered_df.iterrows():
            scores = row['scores']
            metrics = row['metrics']
            display_data.append({
                'Student ID': row['student_id'],
                'Name': row['full_name'],
                'Category': scores['category'],
                'Total Score': scores['total_score'],
                'Base Score': scores['base_scores']['base_total'],
                'Bonus Score': scores['bonus_scores']['bonus_total'],
                'Projects': metrics['project_count'],
                'Hard Skills': metrics['hard_skill_count'],
                'Soft Skills': metrics['soft_skill_count'],
                'Work Experience': metrics['work_experience_count'],
                'Achievements': metrics['achievement_count'],
                'CGPA': metrics['cgpa']
            })

        display_df = pd.DataFrame(display_data)

        # Create a new column 'Secondary Score' for secondary sorting
        display_df['Secondary Score'] = (display_df['Work Experience'] +
                                        display_df['Projects'] +
                                        display_df['Hard Skills'] +
                                        display_df['Soft Skills'] +
                                        display_df['Achievements'])

        # Perform primary and secondary sorting
        sorted_df = display_df.sort_values(by=['Total Score', 'Secondary Score'], ascending=[False, False])

        # Display the dataframe
        st.dataframe(sorted_df, use_container_width=True, hide_index=True)

        st.markdown("<hr style='border:1px solid #ccc;'>", unsafe_allow_html=True)

        # Enhanced bar chart for average scores by category with different colors
        st.subheader("📊 **Average Scores by Category**")
        avg_scores_by_category = (
            pd.DataFrame(display_data)
            .groupby('Category')['Total Score']
            .mean()
            .sort_values(ascending=False)
        )

        # Assign a unique color for each category
        colors = px.colors.qualitative.Set2  # You can choose other color palettes from plotly

        fancy_bar_chart = go.Figure()

        for i, category in enumerate(avg_scores_by_category.index):
            fancy_bar_chart.add_trace(
                go.Bar(
                    x=[category],
                    y=[avg_scores_by_category[category]],
                    name=category,
                    marker=dict(color=colors[i % len(colors)])
                )
            )

        fancy_bar_chart.update_layout(
            title="Average Scores by Category",
            xaxis_title="Category",
            yaxis_title="Average Score",
            template="plotly_white",
            plot_bgcolor="rgba(0, 0, 0, 0.02)",
            legend_title_text=None,
            showlegend=True  # Hide legend if not necessary
        )

        st.plotly_chart(fancy_bar_chart,key="unique_chart_2")


    else:
        st.info("📢 Click 'Begin Display' or 'Process New Students' to begin analysis")

if __name__ == "__main__":
    main()