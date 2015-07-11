from flask import Flask, jsonify, render_template, request
from crawler import db
import handlers
application = Flask(__name__, static_url_path='')

@application.route('/')
def root():
    return render_template('index.html')
    
@application.route('/<college>/<start>/<end>/<threshold>')
def create_graph(college, start, end, threshold):
    return jsonify(data=handlers.create_graph_handler(
        college, start, end, float(threshold)))

@application.route('/colleges')
def get_colleges():
    return jsonify(data=handlers.get_colleges_handler())

@application.route('/post/<_id>')
def get_post(_id):
    return jsonify(data=handlers.get_post_handler(_id))

@application.route('/comment/<_id>')
def get_comment(_id):
    return jsonify(data=handlers.get_comment_handler(_id))

@application.route('/posts/comments/<_id>')
def get_comments(_id):
    return jsonify(data=handlers.get_comments_handler(_id))
if __name__ == "__main__":
    application.run(debug=True)