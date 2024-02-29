import requests
import pandas as pd
import psycopg2


def fetch_questions_and_answers(tagged='Pyspark', site='stackoverflow', sort='activity', order='desc', limit=10):
    """
    Fetches questions and their answers from the Stack Exchange API based on specified parameters.

    Parameters:
    - tagged (str): Tags to filter the questions by.
    - site (str): Stack Exchange site to query (default is 'stackoverflow').
    - sort (str): Sort order of the questions (default is 'activity').
    - order (str): Order of sorting ('asc' for ascending, 'desc' for descending).
    - limit (int): Maximum number of questions to retrieve (default is 10).

    Returns:
    - pandas DataFrame: DataFrame containing question-answer pairs retrieved from the API.
    """
    url = 'https://api.stackexchange.com/2.3/questions'
    params = {
        'pagesize': limit,
        'order': order,
        'sort': sort,
        'tagged': tagged,
        'site': site,
        'filter': 'withbody'  # Include the body of the questions and answers
    }

    response = requests.get(url, params=params)
    if response.status_code == 200:
        data = response.json()
        questions = data.get('items', [])
        question_answers = []

        for question in questions:
            question_id = question['question_id']
            answer_url = f'https://api.stackexchange.com/2.3/questions/{question_id}/answers'
            answer_params = {
                'pagesize': 1,  # Limit to 1 answer per question
                'order': 'desc',
                'sort': 'votes',
                'site': site,
                'filter': 'withbody'  # Include the body of the answers
            }

            answer_response = requests.get(answer_url, params=answer_params)
            if answer_response.status_code == 200:
                answer_data = answer_response.json()
                answers = answer_data.get('items', [])
                if answers:
                    question_content = f"{question['title']} {question['body']} Tags: {' '.join(question['tags'])}"
                    question_answers.append({
                        'QuestionID': question_id,
                        'Question Content': question_content,
                        'Answer Body': answers[0]['body'],  # Take the first answer (highest voted)
                        'Category': tagged
                    })

        df = pd.DataFrame(question_answers)
        return df
    else:
        print(f"Failed to fetch questions and answers. Status code: {response.status_code}")
        return pd.DataFrame()

def check_existing_records(**kwargs):
    # Conectar a la base de datos PostgreSQL
    connection = psycopg2.connect(
        host='localhost',
        database='stack_overflow',
        user='postgres',
        password='123456'
    )
    
    cursor = connection.cursor()
    cursor.execute("SELECT * FROM your_table")
    existing_records = cursor.fetchall()
    
    # Insertar registros nuevos en la base de datos
    new_records = kwargs['task_instance'].xcom_pull(task_ids='fetch_data_task')
    for index, row in new_records.iterrows():
        if row['question_id'] not in [record[0] for record in existing_records]:
            cursor.execute("INSERT INTO your_table (question_id, input_text, output_text, category) VALUES (%s, %s, %s, %s)", (row['question_id'], row['input_text'], row['output_text'], row['category']))
    
    connection.commit()
    connection.close()