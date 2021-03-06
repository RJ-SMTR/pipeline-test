import os
import csv
from dagster import (DagsterType, dagster_type_loader, Field, String, 
                     TypeCheck, EventMetadataEntry, ExpectationResult)

SimpleDataFrame = DagsterType(
    name="SimpleDataFrame",
    type_check_fn=lambda _, value: isinstance(value, list),
    description="A naive representation of a data frame, e.g., as returned by csv.DictReader.",
)

def less_simple_data_frame_type_check(_, value):
    if not isinstance(value, list):
        return False

    fields = [field for field in value[0].keys()]

    for i in range(len(value)):
        row = value[i]
        if not isinstance(row, dict):
            return False
        row_fields = [field for field in row.keys()]
        if fields != row_fields:
            return False

    return True

def less_simple_data_frame_type_check(_, value):
    if not isinstance(value, list):
        return TypeCheck(
            success=False,
            description=(
                "LessSimpleDataFrame should be a list of dicts, got "
                "{type_}"
            ).format(type_=type(value)),
        )

    fields = [field for field in value[0].keys()]

    for i in range(len(value)):
        row = value[i]
        if not isinstance(row, dict):
            return TypeCheck(
                success=False,
                description=(
                    "LessSimpleDataFrame should be a list of dicts, "
                    "got {type_} for row {idx}"
                ).format(type_=type(row), idx=(i + 1)),
            )
        row_fields = [field for field in row.keys()]
        if fields != row_fields:
            return TypeCheck(
                success=False,
                description=(
                    "Rows in LessSimpleDataFrame should have the same fields, "
                    "got {actual} for row {idx}, expected {expected}"
                ).format(actual=row_fields, idx=(i + 1), expected=fields),
            )

    return TypeCheck(
        success=True,
        description="LessSimpleDataFrame summary statistics",
        metadata_entries=[
            EventMetadataEntry.text(
                str(len(value)),
                "n_rows",
                "Number of rows seen in the data frame",
            ),
            EventMetadataEntry.text(
                str(len(value[0].keys()) if len(value) > 0 else 0),
                "n_cols",
                "Number of columns seen in the data frame",
            ),
            EventMetadataEntry.text(
                str(list(value[0].keys()) if len(value) > 0 else []),
                "column_names",
                "Keys of columns seen in the data frame",
            ),
        ],
    )

LessSimpleDataFrame = DagsterType(
    name="LessSimpleDataFrame",
    description="A more sophisticated data frame that type checks its structure.",
    type_check_fn=less_simple_data_frame_type_check,
)

@dagster_type_loader({"csv_path": Field(String)})
def less_simple_data_frame_loader(context, config):
    csv_path = os.path.join(os.path.dirname(__file__), config["csv_path"])
    with open(csv_path, "r") as fd:
        lines = []
        for row in csv.DictReader(fd):
            #row["calories"] = int(row["calories"])
            lines.append(row)

    context.log.info("Read {n_lines} lines".format(n_lines=len(lines)))
    return lines

ReadSimpleDataFrame = DagsterType(
    name="LessSimpleDataFrame",
    description="A more sophisticated data frame that type checks its structure.",
    type_check_fn=less_simple_data_frame_type_check,
    loader=less_simple_data_frame_loader,
)

def expect_column_to_be_integers(
    data_frame: LessSimpleDataFrame, column_name: str
) -> ExpectationResult:
    bad_values = []
    for idx in range(len(data_frame)):
        line = data_frame[idx]
        if not isinstance(line[column_name], int):
            bad_values.append((idx, str(line[column_name])))
    return ExpectationResult(
        success=(not bad_values),
        label="col_{column_name}_is_int".format(column_name=column_name),
        description=(
            "Check whether type of column {column_name} in "
            "LessSimpleDataFrame is int"
        ).format(column_name=column_name),
        metadata_entries=[
            EventMetadataEntry.json(
                {"index": idx, "bad_value": value},
                "bad_value",
                "Bad value in column {column_name}".format(
                    column_name=column_name
                ),
            )
            for (idx, value) in bad_values
        ],
    )