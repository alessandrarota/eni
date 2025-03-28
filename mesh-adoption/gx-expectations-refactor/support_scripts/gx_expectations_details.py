import inspect
import great_expectations as gx
import pandas as pd
import json

classes = [name for name, obj in inspect.getmembers(gx.expectations.core, inspect.isclass)]
print(f"Classes: {len(classes)}")


def analyze_for_basic_case(cls):
    doc = getattr(cls, "__doc__", "") or ""
    passing, failing = extract_cases(doc)
    _validate_method = extract_validate_method(cls)
    return {
        "class": cls.__name__,
        "passing_case": passing,
        "failing_case": failing,
        "_validate": _validate_method
    }

def extract_cases(doc):
    return extract_output_after(doc, "Passing Case:"), extract_output_after(doc, "Failing Case:")

def extract_output_after(doc, case_label):
    if case_label in doc:
        start = doc.find(case_label) + len(case_label)
        output_start = doc.find("Output:", start) + len("Output:")
        if output_start == -1: return {}
        output_text = doc[output_start:].strip()
        if case_label == "Passing Case:" and "Failing Case:" in output_text:
            output_text = output_text.split("Failing Case:")[0].strip()
        try:
            return json.loads(output_text)
        except json.JSONDecodeError as e:
            print(e)
            return {"error": "Invalid JSON", "text": output_text} 
    return {}

def extract_validate_method(cls):
    if hasattr(cls, "_validate"):
        return inspect.getsource(cls._validate)
    return None

def analyze_classes_for_basic_case(module):
    all_classes_data = [analyze_for_basic_case(obj[1]) for obj in inspect.getmembers(module, inspect.isclass)]
    with open("classes_basic_data.json", "w") as json_file:
        json.dump(all_classes_data, json_file, indent=4)
    create_excel_with_basic_data(all_classes_data)

def create_excel_with_basic_data(classes_data):
    excel_data = [{
        "class": class_data["class"],
        "passing_case": json.dumps(class_data["passing_case"], indent=4),
        "failing_case": json.dumps(class_data["failing_case"], indent=4)
    } for class_data in classes_data]
    pd.DataFrame(excel_data).to_excel("classes_basic_data.xlsx", index=False)

analyze_classes_for_basic_case(gx.expectations.core)


