from subprocess import call
from email.mime.text import MIMEText
import datetime, sys, csv, smtplib

class DictDiffer(object):
    """
    Calculate the difference between two dictionaries as:
    (1) items added
    (2) items removed
    (3) keys same in both but changed values
    (4) keys same in both and unchanged values
    Totally stole this from StackOverflow
    """
    def __init__(self, current_dict, past_dict):
        self.current_dict, self.past_dict = current_dict, past_dict
        self.set_current, self.set_past = set(current_dict.keys()), set(past_dict.keys())
        self.intersect = self.set_current.intersection(self.set_past)
    def added(self):
        return self.set_current - self.intersect 
    def removed(self):
        return self.set_past - self.intersect 
    def changed(self):
        return set(o for o in self.intersect if self.past_dict[o] != self.current_dict[o])
    def unchanged(self):
        return set(o for o in self.intersect if self.past_dict[o] == self.current_dict[o])

today = datetime.date.today()
todayFileString = 'search-tests/result-%s.csv' % today.isoformat()
result = call(['python', 'testSearchWithCsv.py', 'search-test-cases.csv', todayFileString])

if result != 0:
    #todo mail this
    print 'Received error code', result, 'from script.'
    sys.exit()

yesterday = today - datetime.timedelta(1)

with open('search-tests/result-%s.csv' % (yesterday.isoformat()), 'r') as yesterdayfile:
    todayFile = open(todayFileString, 'r')
    
    yesterdayReader = csv.reader(yesterdayFile)
    todayReader = csv.reader(todayFile)

    response = []

    oldscore = yesterdayReader.next()[1]
    newscore = todayReader.next()[1]
    if oldscore != newscore:
        response.append("Scoring has changed from %s to %s" % (str(oldscore), str(newscore)))

    differDict = { 'yesterday' : {}, 'today' : {} }

    yesterdayReader.next()
    todayReader.next()

    for row in yesterdayReader:
        if row[0]:
            grouping = (row[0], row[1])
            differDict['yesterday'][grouping] = {}
            differDict['yesterday'][grouping]['score'] = row[2]
        else:
            differDict['yesterday'][grouping][row[3]] = row[4:]

    for row in todayReader:
        if row[0]:
            grouping = (row[0], row[1])
            differDict['today'][grouping] = {}
            differDict['today'][grouping]['score'] = row[2]
        else:
            differDict['today'][grouping][row[3]] = row[4:]

    dd = DictDiffer(differDict['yesterday'], differDict['today'])
    for changed in dd.changed():
        response.append("For wiki %s and query '''%s''':" % (changed[0], changed[1]))
        response.append("\tYesterday's score: %s" % (differDict['yesterday'][changed]['score']))
        response.append("\tYesterday's expected results: %s" % (', '.join(differDict['yesterday'][changed]['Expected:'])))
        response.append("\tYesterday's actual results: %s" % (', '.join(differDict['yesterday'][changed]['Actual:'])))
        response.append("\tToday's score: %s" % (differDict['today'][changed]['score']))
        response.append("\tToday's expected results: %s" % (', '.join(differDict['today'][changed]['Expected:'])))
        response.append("\tToday's actual results: %s" % (', '.join(differDict['today'][changed]['Actual:'])))
        response.append("")

    fromEmail = "no-reply@wikia-inc.com"
    toEmail = "robert@wikia-inc.com"

    message = MIMEText("\n".join([r for r in  response]))
    message['Subject'] = "Search report for %s (score of %s)" % (today.isoformat(), str(newscore))
    message['From'] = fromEmail
    message['To'] = toEmail

    s = smtplib.SMTP('localhost')
    s.sendmail(fromEmail, [toEmail], message.as_string())
    s.quit()
