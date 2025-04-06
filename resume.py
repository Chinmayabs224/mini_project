import re
import json
import pdfplumber
import spacy
from collections import defaultdict

# Initialize NLP model
nlp = spacy.load("en_core_web_sm")

# Technical skills taxonomy
TECH_SKILLS = {
    'Big Data': ['hadoop', 'spark', 'hive', 'kafka', 'flink', 'hbase', 
                'cassandra', 'bigquery', 'data lake', 'data warehouse'],
    'Machine Learning': ['machine learning', 'deep learning', 'neural networks',
                       'tensorflow', 'pytorch', 'scikit-learn', 'xgboost',
                       'computer vision', 'nlp', 'natural language processing'],
    'Programming': ['python', 'java', 'scala', 'r', 'c++', 'sql'],
    'Cloud': ['aws', 'azure', 'gcp', 'docker', 'kubernetes'],
    'Data Engineering': ['etl', 'data pipeline', 'airflow', 'data modeling']
}

def extract_text_from_pdf(pdf_path):
    """Extract text from PDF with error handling"""
    try:
        with pdfplumber.open(pdf_path) as pdf:
            return "\n".join(page.extract_text() for page in pdf.pages if page.extract_text())
    except Exception as e:
        raise Exception(f"PDF processing failed: {str(e)}")

def extract_personal_info(text):
    """Extract name, email, phone with improved patterns"""
    email = re.search(r'[\w\.-]+@[\w\.-]+\.[a-zA-Z]{2,}', text)
    phone = re.search(
        r'(?:\+?\d{1,3}[-.\s]?)?\(?\d{2,3}\)?[-.\s]?\d{3}[-.\s]?\d{4}',
        text
    )
    
    # Name extraction using NLP
    doc = nlp(text[:500])  # Only analyze first part for performance
    name = next((ent.text for ent in doc.ents if ent.label_ == "PERSON"), None)
    
    return {
        'name': name,
        'email': email.group() if email else None,
        'phone': phone.group() if phone else None
    }

def extract_education(text):
    """Extract education information with degree focus"""
    education = []
    pattern = re.compile(
        r'(?P<degree>bachelor|master|ph\.?d|m\.?tech|b\.?tech|mba|msc?|bsc?)\s*(?:in|of)?\s*(?P<field>[\w\s]+)\s*'
        r'(?:at|from|,)\s*(?P<institution>[^\n]+?)\s*'
        r'(?P<year>\(?\d{4}\s*[-–]\s*(?:\d{4}|present|current)\)?)',
        re.IGNORECASE
    )
    
    for match in pattern.finditer(text):
        education.append({
            'degree': match.group('degree').title(),
            'field': match.group('field').title(),
            'institution': match.group('institution'),
            'duration': match.group('year')
        })
    
    return education

def extract_work_experience(text):
    """Extract work experience with company and duration"""
    experience = []
    # Improved pattern to capture job titles with various formats
    pattern = re.compile(
        r'(?P<title>[A-Z][\w\s]+(?:Engineer|Developer|Analyst|Scientist|Manager))\s*'
        r'(?:at|@|,)\s*(?P<company>[^\n,;]+?)\s*'
        r'(?P<duration>\(?\d{4}\s*[-–]\s*(?:\d{4}|present|current)\)?)',
        re.IGNORECASE
    )
    
    for match in pattern.finditer(text):
        experience.append({
            'position': match.group('title'),
            'company': match.group('company').strip(),
            'duration': match.group('duration')
        })
    
    return experience

def extract_technical_skills(text):
    """Categorize technical skills with level detection"""
    skills_found = defaultdict(list)
    text_lower = text.lower()
    
    for category, skills in TECH_SKILLS.items():
        for skill in skills:
            if re.search(r'\b' + re.escape(skill) + r'\b', text_lower):
                # Detect skill level if mentioned
                level = None
                level_match = re.search(
                    fr'{re.escape(skill)}.*?(advanced|intermediate|expert|beginner|proficient)',
                    text_lower
                )
                if level_match:
                    level = level_match.group(1)
                
                skills_found[category].append({
                    'skill': skill,
                    'level': level
                })
    
    return dict(skills_found)

def extract_projects(text):
    """Extract projects with technologies used"""
    projects = []
    # Pattern to capture project sections
    project_sections = re.split(r'\n\s*(?:projects|work experience|experience)\s*\n', text, flags=re.IGNORECASE)
    
    if len(project_sections) > 1:
        project_text = project_sections[1]
        # Split individual projects
        project_items = re.split(r'\n\s*(?=\w)', project_text)
        
        for item in project_items:
            if not item.strip():
                continue
                
            # Extract project name
            name_match = re.match(r'^(.*?)[:-]', item)
            name = name_match.group(1).strip() if name_match else "Unnamed Project"
            
            # Extract technologies used
            technologies = []
            for category in TECH_SKILLS.values():
                for tech in category:
                    if re.search(r'\b' + re.escape(tech) + r'\b', item.lower()):
                        technologies.append(tech)
            
            projects.append({
                'name': name,
                'description': item.strip(),
                'technologies': technologies
            })
    
    return projects

def analyze_resume(pdf_path):
    """Main function to analyze resume and extract structured data"""
    try:
        text = extract_text_from_pdf(pdf_path)
        if not text:
            return {"error": "No text could be extracted from the PDF"}
        
        result = {
            'personal_info': extract_personal_info(text),
            'education': extract_education(text),
            'work_experience': extract_work_experience(text),
            'skills': extract_technical_skills(text),
            'projects': extract_projects(text)
        }
        
        # Calculate total experience in years
        total_exp = 0
        for exp in result['work_experience']:
            years = re.findall(r'\d{4}', exp['duration'])
            if len(years) == 2:
                total_exp += (int(years[1]) - int(years[0]))
        result['total_experience_years'] = total_exp
        
        return result
    
    except Exception as e:
        return {"error": f"Resume analysis failed: {str(e)}"}

if __name__ == "__main__":
    # Example usage
    pdf_path = "resume.pdf"  # Replace with your PDF path
    
    result = analyze_resume("C:/Users/ASUS/Downloads/UdayKumarCV.befed8a0c1e7c77d9bf9.pdf")
    
    if 'error' in result:
        print(f"Error: {result['error']}")
    else:
        print(json.dumps(result, indent=2))