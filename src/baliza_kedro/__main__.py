from kedro.framework.session import KedroSession
from kedro.framework.startup import bootstrap_project

def main():
    metadata = bootstrap_project(".")
    with KedroSession.create(metadata.package_name) as session:
        session.run()

if __name__ == "__main__":
    main()
