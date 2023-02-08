import os

#Are we developing in development mode? Loading file from .env folder in root directory. https://pypi.org/project/python-dotenv/
def is_development():
    if os.getenv("PRODUCTION") == None:
        from dotenv import load_dotenv
        load_dotenv()
        print(r"""
              ****************************************************************
              PRODUCTION NOT SET, USING .env IN ROOT FOR DEVELOPMENT VARIABLES.
              ****************************************************************
              """)

    else:
        print(r"""
              *************************************************
              USING PRODUCTION VARIABLES FROM YAML.
              *************************************************
              """)
