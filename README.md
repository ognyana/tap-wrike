# tap-wrike

A singer.io tap for extracting data from the Wrike API, written in python 3.

Author: Ognyana Ivanova

## Quick start

1. Install

    Clone this repository, and then install using setup.py. We recommend using a virtualenv:

    ```bash
    > virtualenv -p python3 venv
    > source venv/bin/activate
    > python setup.py install
    ```

2. Create your tap's config file which should look like the following:

    ```json
    { 
       "access_token": "WRIKE_ACCESS_TOKEN"
    }
    ```

43. Run the application

    `tap-wrike` can be run with:

    ```bash
    tap-wrike --config config.json
    ```

---

Copyright &copy; 2021
