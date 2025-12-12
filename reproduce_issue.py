import dlt
from dlt.extract.resource import DltResource
from typing import cast

def reproduce():
    @dlt.resource(name="test")
    def my_resource():
        yield {"id": 1}

    @dlt.source(name="my_source")
    def my_source_func():
        return [my_resource()]

    source = my_source_func()

    print("Iterating source.resources:")
    for key in source.resources:
        print(f"Key: {key}, type: {type(key)}")
        resource = source.resources[key]
        print(f"Resource type: {type(resource)}")

        resource_obj = cast(DltResource, resource)
        if resource_obj._pipe.has_parent:
             print("Has parent")
        else:
             print("No parent")

if __name__ == "__main__":
    reproduce()
