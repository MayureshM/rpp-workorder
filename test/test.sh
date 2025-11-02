#!/bin/bash

case $1 in

  unit_test)
    #We will run unit tests here.
    echo "Unit Tests will be executed now..."
    python -m pytest --alias="${ALIAS}"

    ;;

  integration_test)
    #We will run integration tests here.
    echo "Integration Tests will be executed now..."
#    python -m pytest --alias="${ALIAS}"

    ;;
  regression_test)
    #We will run regression tests here.
    echo "Regression Tests will be executed now..."
#    python -m pytest --alias="${ALIAS}"

    ;;

  *)
    STATEMENTS
    ;;
esac

