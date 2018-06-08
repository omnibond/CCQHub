from bottle import run
import argparse

parser = argparse.ArgumentParser(description="Runs the ccqHub webserver that accepts requests for job submission.")
parser.add_argument('-V', '--version', action='version', version='ccqHubWebServer (version 0.1)')
parser.add_argument('-ht', '--host', help="The IP address that ccqHub is going to listen on. By default this is set to localhost. For AWS based installations of ccqHub the IP address needs to be 0.0.0.0 if ccqHub is to use the DNS name/public IP address for your instance.", default="localhost")
parser.add_argument('-p', '--port', help="The port number that ccqHub is going to listen for requests on. The default is port 8080.", default=8080)

args = parser.parse_args()
host = args.host
port = args.port

# TODO Make sure that the reloader=True command gets taken out, should only be used for debugging!
run(host=str(host), port=int(port), server="tornado", reloader=True)
