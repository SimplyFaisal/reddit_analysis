from flask import Flask, jsonify, render_template, request
from crawler import db
import handlers
application = Flask(__name__, static_url_path='')

@application.route('/')
def root():
    return render_template('index.html')
    
@application.route('/<college>')
def create_graph(college):
    day = request.args.get('day')
    threshold = float(request.args.get('threshold'))
    return jsonify(data=handlers.create_graph_handler(college, day, threshold))

if __name__ == "__main__":
    application.run(debug=True)