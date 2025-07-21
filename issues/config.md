## `config.py`: Hardcoded PNCP Endpoint Configuration

### Problem

The `pncp_endpoints` list is hardcoded within the `Settings` class in `src/baliza/config.py`. While this works, it has a few drawbacks:

1.  **Reduced Flexibility**: Users who want to add, remove, or modify endpoints must directly edit the Python source code. This is not ideal, especially for users who are not developers.
2.  **Maintenance Overhead**: As the number of endpoints grows, the `Settings` class can become cluttered, making it harder to maintain.
3.  **Less Declarative**: Configuration is generally better when it's declarative (e.g., in a YAML or JSON file) rather than imperative (in code).

### Potential Solutions

1.  **Move Endpoint Configuration to a YAML File**:
    *   Create a `endpoints.yaml` file (or similar) that contains the list of endpoint configurations.
    *   Modify the `Settings` class to load this YAML file. Pydantic can be extended to support custom file formats, or you can write a simple loader function.
    *   This would allow users to easily customize the endpoints by editing the YAML file.

2.  **Allow Overriding Endpoints via Environment Variables**:
    *   Pydantic supports complex types in environment variables (e.g., by providing a JSON string). You could document how users can override the `pncp_endpoints` list using an environment variable.
    *   This is less user-friendly than a YAML file but doesn't require adding a new file to the project.

### Recommendation

Move the endpoint configuration to a separate `endpoints.yaml` file. This is the most flexible and user-friendly solution. It separates configuration from code and makes the system easier to extend and maintain. The `config/endpoints.yaml` file already exists, so it should be used. The settings loader should be updated to read from this file.
