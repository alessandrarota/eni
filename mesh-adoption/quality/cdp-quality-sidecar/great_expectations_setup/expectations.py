data_product_name = "consuntiviDiProduzione"

data_product_suites = [
    {
        "physical_informations": {
            "data_source_name": "cdpDataSourceSample",
            "data_asset_name": "cdpDataAssetSample",
            "dataframe": "https://raw.githubusercontent.com/great-expectations/gx_tutorials/main/data/yellow_tripdata_sample_2019-01.csv"
        },
        "expectations": [
            {
                "expectation_name": "expectVendorIdValuesToNotBeNull",
                "expectation_type": "expect_column_values_to_not_be_null",
                "kwargs": {
                    "column": "vendor_id"
                }
            },
            {
                "expectation_name": "expectPassengerCountValuesToBeBetween",
                "expectation_type": "expect_column_values_to_be_between",
                "kwargs": {
                    "column": "passenger_count",
                    "min_value": 0,
                    "max_value": 4
                }
            },
            {
                "expectation_name": "expectPickupDatetimeValuesToMatchRegex",
                "expectation_type": "expect_column_values_to_match_regex",
                "kwargs": {
                    "column": "pickup_datetime",
                    "regex": r"^\d{4}-(0[1-9]|1[0-2])-(0[1-9]|[12][0-9]|3[01])\s([01]\d|2[0-3]):([0-5]\d):([0-5]\d)$"
                }
            }
        ]
    }#,
    # {
    #     "physical_informations": {
    #         "data_source_name": "cdpDataSourceSample2",
    #         "data_asset_name": "cdpDataAssetSample2",
    #         "dataframe": "https://raw.githubusercontent.com/great-expectations/gx_tutorials/main/data/yellow_tripdata_sample_2019-01.csv"
    #     },
    #     "expectations": [
    #         {
    #             "expectation_name": "expectVendorIdValuesToNotBeNull",
    #             "expectation_type": "expect_column_values_to_not_be_null",
    #             "kwargs": {
    #                 "column": "vendor_id"
    #             }
    #         },
    #         {
    #             "expectation_name": "expectPassengerCountValuesToBeBetween",
    #             "expectation_type": "expect_column_values_to_be_between",
    #             "kwargs": {
    #                 "column": "passenger_count",
    #                 "min_value": 0,
    #                 "max_value": 4
    #             }
    #         },
    #         {
    #             "expectation_name": "expectPickupDatetimeValuesToMatchRegex",
    #             "expectation_type": "expect_column_values_to_match_regex",
    #             "kwargs": {
    #                 "column": "pickup_datetime",
    #                 "regex": r"^\d{4}-(0[1-9]|1[0-2])-(0[1-9]|[12][0-9]|3[01])\s([01]\d|2[0-3]):([0-5]\d):([0-5]\d)$"
    #             }
    #         }
    #     ]
    # }
]
