import ibm_db

cpq_db_conn = '<hidden due to confidentiality purposes>'

espr_db_conn = '<hidden due to confidentiality purposes>'


def generate_cpq_cursor():
    try:
        con = ibm_db.connect(cpq_db_conn, "", "")
        return con
    except Exception:
        print("Could not estabilish connection with CPQ DB")


def generate_espr_cursor():
    try:
        con = ibm_db.connect(espr_db_conn, "", "")
        return con
    except Exception:
        print("Could not estabilish connection with ESPR DB.")
