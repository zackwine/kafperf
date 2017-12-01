# Test Environment

A few notes about the test environment

 - Requires docker running on the development machine
 - Launches a few containers that run kafka, zookeeper, and a consumer
 - Creates a topic "test", and creates a consumer for that topic


## Usage

Run the following from the testenv directory to bring up the test environment:
  
    docker-compose up

To clean up the test environment run the following:

    docker-compose down

