# rpp-workorder

Note: boto3 is not included in the requirements.txt files in order to reduce the size of our Lambda functions (< 10MB and we can edit functions in the console).
It is required and DEVs should be using the currently supported version of boto3 listed [here](https://docs.aws.amazon.com/lambda/latest/dg/current-supported-versions.html).


To test the callbacks locally:

```bash
$ ./run.sh -r
# => starts a local api
$ CTRL+C
# => exit api

$ sam local invoke "RPPWorkorderConditionCallback" -e tests/data/orders_condition_created.json  --env-vars=env.json --docker-network rpp-workorder_local-build
```

## Unit Tests

Install test dependencies in [tests/requirements.txt](tests/requirements.txt)

```bash
python setup.py develop
pytest -v
```
