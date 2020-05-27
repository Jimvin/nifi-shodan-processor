# nifi-shodan-processor
This custom processor connects to the https://shodan.io API and streams events from the /shodan/banners and
/shodan/alerts endpoints. It is based heavily on the getTwitter processor, since this already has the code
needed to connect to an API and stream the resulting records.

## Installation
Run `mvn install` from the root directory of this project to create a NAR file that you can install on your nifi
server. Copy the NAR file from nifi-shodan-processor/nifi-shodan-nar/target/nifi-shodan-nar-<version>-SNAPSHOT.nar
to /var/lib/nifi/extensions on your Nifi server and restart.

## Usage

This package provides two processors:
- ShodanStreamingProcessor
- ShodanRestProcessor (work in progress)

For now only the ShodanStreamingProcessor is fully implemented. The ShodanRestProcessor is intended to make calls
to the non-streaming API endpoint, for example to run queries on Shodan and return the results in flowfiles.

Under properties you must set your Shodan API key and choose either the banner or the alert endpoint. For alerts
you must also set the Shodan alert ID you wish to subscribe to.

The output from the processor are the raw JSON result from the Shodan API. 
