try:
    from bs4 import BeautifulSoup
except:
    print ('Install bs4 (pip install bs4)')
try:
    from urllib.request import *
except:
    from urllib2 import *
import sqlite3
import pandas as pd


class UrlOpener(object):
    def __init__(self, url):
        self.url = url

    def contents(self):
        return BeautifulSoup(urlopen(self.url), "html.parser")


class StockDB(object):
    def __init__(self):
        self.connector = sqlite3.connect('stocks.db')
        self.cursor = self.connector.cursor()

        self.cursor.execute('''CREATE TABLE IF NOT EXISTS
                 STOCKSDATA
                (
                Company TEXT PRIMARY KEY NOT NULL,
                Market_Cap REAL NOT NULL,
                Total_Asset REAL NOT NULL,
                Net_Profit REAL NOT NULL,
                Sector TEXT NOT NULL
                );''')

        self.connector.commit()

    def writeData(self, values):
        try:
            self.cursor.execute("""
            INSERT INTO
                STOCKSDATA
                ( Company, Market_Cap, Total_Asset, Net_Profit, Sector)
                VALUES( ?,?,?,?,? );
                """, values)

            self.connector.commit()
        except sqlite3.IntegrityError:
            print ('Already Exists Skipped...')

    def companyList(self):
        res = []
        for row in self.cursor.execute('SELECT Company FROM STOCKSDATA ORDER BY Company'):
            res.append(row[0])
        return res

    def third_4th_highest_market_cap_sectorwise(self):
        query1 = """SELECT *, ROW_NUMBER()OVER(PARTITION BY Sector ORDER BY Market_Cap DESC) rn FROM STOCKSDATA"""

        query2 = 'SELECT Company, Market_Cap, Sector FROM(' + query1 + ')' + 'WHERE rn IN (' + '3, 4' + ')'
        data = self.cursor.execute(query2)

        df = pd.DataFrame(data, columns=['Company', 'Market_Cap', 'Sector'])

        df.to_csv('3rd_4th_market_cap.csv', sep='|', index=False)

    def get_P_ratio_E_in_interval(self):
        start = 11
        max = self.cursor.execute('''SELECT max(Market_Cap/Net_Profit) as PE from STOCKSDATA''').fetchone()[0]
        data_list = []
        while (start < max):
            query = '''SELECT Company, (Market_Cap/Net_Profit) as PE from STOCKSDATA where PE BETWEEN ''' + str(
                start) + ''' AND ''' + str(start + 5);
            data = self.cursor.execute(query).fetchall()
            if data:
                data_list.append((str(start) + '-' + str(start + 5), ','.join([company[0] for company in data])))
            start += 5

        df = pd.DataFrame(data_list, columns=['interval', 'company_list'])

        df.to_csv('company_with_interval.csv', sep='|', index=False)

    def __del__(self):
        print ('Closing Database')
        self.cursor.close()
        self.connector.close()


def GrabStocks():
    DB = StockDB()
    test_sufix = '.+'  # 'auto-cars-jeeps' # 'chemicals' # 'cement-major' # 'computers-software' # 'auto-cars-jeeps' #
    soup = UrlOpener(r'http://www.moneycontrol.com/stocks/sectors/').contents()
    scrapPattern_0 = soup("a", {'href': re.compile(r'/stocks/sectors/%s\.html$' % test_sufix)})
    visited = {}
    companyList = DB.companyList()

    for sites in scrapPattern_0:

        soup = UrlOpener(sites['href']).contents()

        scrapPattern_1 = soup("a", {'href': re.compile(r'/stockpricequote/.+/.+')})

        if scrapPattern_1:
            print ('Listing all companies from %s Sector' % sites.get_text())

        for stocks in scrapPattern_1:

            compName = stocks.get_text()

            if compName in visited:
                continue
            elif compName in companyList:
                continue
            else:
                visited[stocks.get_text()] = ''

            print ('--- %s' % stocks.get_text())

            soup = UrlOpener(r'http://www.moneycontrol.com/%s' % stocks['href']).contents()

            try:
                try:
                    symbol = soup.find(text=re.compile("NSE\s*:\s*(\S+?)")).replace('NSE:', '').strip()
                except:
                    symbol = 'N/A'
                    print ('Not Traded on NSE %s' % stocks.get_text())
                Market_Cap = soup.find(text='MARKET CAP (Rs Cr)').findNext().get_text(),
                Market_Cap = float(Market_Cap[0].replace(',', ''))
                Total_Asset = float(soup.findAll(text='Total Assets')[1].findNext().get_text().replace(',', ''))
                Net_Profit = soup.find('td', text=re.compile("Net Profit"),
                                       attrs={'class': 'thc02 w160 gD_12'}).findNext().get_text()
                Net_Profit = float(Net_Profit.replace(',', ''))
            except:
                Market_Cap = 1.0
                Total_Asset = 0.0
                Net_Profit = 0.0

            values = [
                stocks.get_text(),
                Market_Cap,
                Total_Asset,
                Net_Profit,
                sites.get_text()
            ]
            DB.writeData(values)


if __name__ == '__main__':
    # GrabStocks()
    DB = StockDB()
    # DB.third_4th_highest_market_cap_sectorwise()
    DB.get_P_ratio_E_in_interval()



