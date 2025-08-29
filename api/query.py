def format_results(results, question):
    """Use LLM to format results into natural language answer"""
    if not results:
        return "No results found for your question."
    
    try:
        question_lower = question.lower()
        
        # For testing without OpenAI key
        if not openai.api_key:
            # ... existing code ...
        
        # Pre-process analytical questions about employees
        if any(phrase in question_lower for phrase in ['which employee', 'most photos', 'employee has the most']):
            # Count by employee
            employee_counts = {}
            for result in results:
                first_name = result.get('Employee First Name', '')
                last_name = result.get('Employee Last Name', '')
                if first_name or last_name:
                    full_name = f"{first_name} {last_name}".strip()
                    employee_counts[full_name] = employee_counts.get(full_name, 0) + 1
            
            if employee_counts:
                # Find top employee
                top_employee = max(employee_counts.items(), key=lambda x: x[1])
                # Create a summary for OpenAI
                summary_text = f"Employee photo counts from {len(results)} total records:\n"
                sorted_employees = sorted(employee_counts.items(), key=lambda x: x[1], reverse=True)[:5]
                for emp, count in sorted_employees:
                    summary_text += f"- {emp}: {count} photos\n"
                
                # Let OpenAI format this nicely
                response = openai.ChatCompletion.create(
                    model="gpt-3.5-turbo",
                    messages=[
                        {
                            "role": "system",
                            "content": "Format the answer clearly and concisely."
                        },
                        {
                            "role": "user",
                            "content": f"Question: {question}\n\nData: {summary_text}\n\nAnswer which employee has the most photos based on this data."
                        }
                    ],
                    temperature=0.1,
                    max_tokens=100
                )
                return response.choices[0].message.content.strip()
        
        # Rest of the existing format_results code...
