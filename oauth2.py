import json

import flask
import httplib2

from apiclient import discovery
from oauth2client import client

app = flask.Flask(__name__)


@app.route('/')
def index():
    if 'credentials' not in flask.session:
        return flask.redirect(flask.url_for('oauth2callback'))
    credentials = client.OAuth2Credentials.from_json(flask.session['credentials'])
    if credentials.access_token_expired:
        return flask.redirect(flask.url_for('oauth2callback'))
    else:
        http_auth = credentials.authorize(httplib2.Http())
        drive = discovery.build('drive', 'v2', http_auth)  # Same as discovery.build set to service in gphotos app
        files = drive.files().list().execute()
        return json.dumps(files)


@app.route('/oauth2callback')
def oauth2callback():
    CLIENT_SECRET_FILE = r'C:\Users\SJackson\Documents\Personal\Programming\flex_py3\client_secret.json'
    SCOPES = 'https://www.googleapis.com/auth/drive.readonly https://www.googleapis.com/auth/drive.photos.readonly'
    flow = client.flow_from_clientsecrets(
        CLIENT_SECRET_FILE, scope=SCOPES,
        redirect_uri=flask.url_for('oauth2callback', _external=True)
    )
    if 'code' not in flask.request.args:
        auth_uri = flow.step1_get_authorize_url()
        return flask.redirect(auth_uri)
    else:
        auth_code = flask.request.args.get('code')
        credentials = flow.step2_exchange(auth_code)
        flask.session['credentials'] = credentials.to_json()
        return flask.redirect(flask.url_for('index'))


if __name__ == '__main__':
    import uuid

    app.secret_key = str(uuid.uuid4())
    app.debug = False
    app.run()
