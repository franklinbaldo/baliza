import ibis

def my_node(input_data: ibis.Table) -> ibis.Table:
    return input_data.mutate(new_column=1)
