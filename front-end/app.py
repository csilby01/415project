from flask import Flask, jsonify, request, render_template

app = Flask(__name__)

@app.route('/')
def index():
    return render_template('index.html')

@app.route('/find', methods=['POST'])
def filter():
    combined_df = combine_tables()
    data = request.json() ## change from json to a list of strings
    filtered_results = show_airports(data, combined_df) ## return a filtered data frame

    ## Print filtered results on page
    response = {'success': True, 'filtered_results': filtered_results}
    return jsonify(response)


if __name__ == '__main__':
    app.run(debug=True, port=3000)