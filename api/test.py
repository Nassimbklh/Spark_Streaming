from opensky_api import OpenSkyApi
api = OpenSkyApi()
s = api.get_states()
print(s)