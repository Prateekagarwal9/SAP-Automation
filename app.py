from flask import Flask, request,render_template
import json
from schema_export import ViewSchemaExporter
from df_creator import DFCreator
app = Flask(__name__)

@app.route('/create', methods=['POST'])
def test():
    rgname = request.form['rgname']
    dfname = request.form['dfname']
    subscriptionid = request.form['subscriptionid']
    clientid = request.form['clientid']
    tenantid = request.form['tenantid']
    secret = request.form['secret']
    viewlist = request.form['viewlist'].split(',')
    hana_con_string = request.form['hana_con_string']
    sql_con_string = request.form['sql_con_string']

    for view in viewlist:
        se = ViewSchemaExporter(hana_con_string,sql_con_string,view)
        se.replicate_sap_view()
        mapping = se.mapping
        dc = DFCreator(subscriptionid,rgname,dfname,clientid,secret,tenantid,mapping,view)
        
        print(se)
    return str(dc)


@app.route('/', methods=['GET'])
def root():
    return render_template('form.html')


if __name__ == '__main__':
    app.run(host='0.0.0.0',debug=True)