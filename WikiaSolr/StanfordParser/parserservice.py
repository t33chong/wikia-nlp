import jsonrpclib, json
from parsedoutput import ParsedOutput

class ParserService:
    def __init__(self, url = "http://localhost:8080"):
        self.server = jsonrpclib.Server(url)

    def parse(self, text):
        print 'PARSING'
        try:
            return ParsedOutput(json.loads(self.server.parse(text)))
        except:
            return ParsedOutput({})

if __name__ == '__main__':
    ps = ParserService()
    parsed = ps.parse("""The Lunar Lander is a utility that appears in Ascension. It takes players from a launch pad on the map back to the centrifuge while letting them shoot zombies below. It costs 250 points to use and can be called to a certain landing spot. """)
