import pandas as pd

class ResultFormatter:

    def __init__(self):
        self.result_fields = {
            "element_count": None,
            "unexpected_count": None,
            "unexpected_percent": None,
            "expectation_output_metric": None
        }

    def format_result(self, input_dict):
        print(input_dict)

        if "result" in input_dict:
            result_data = input_dict["result"]
            formatted_result = {key: result_data.get(key, None) for key in self.result_fields}
            input_dict["result"] = formatted_result

        return input_dict