from os import path
from threading import Thread

from data_clusterer import Scraper, QuantClusterer
from cluster_trader import ClusterTrader

data_dir = './data'
# For NASDAQ all symbol load
# filenames = ['nasdaqlisted.txt', 'otherlisted.txt']
# paths = [path.join(data_dir, f) for f in filenames]
# history_file = Scraper().download_all(filenames=paths, prev_df=path.join(data_dir, 'out.csv'))
# For smaller symbol list:
# symbol_list = ['OAS', 'NOVN', 'TRVN', 'AVGR', 'EVFM', 'FCEL', 'CTRM', 'GEVO', 'SXTC', 'HTBX', 'VEON', 'VBIV',
#                 'NAKD', 'GNUS', 'OPK', 'ZSAN', 'CLUB', 'NMTR', 'CIDM', 'CHNR', 'AMRN', 'CDEV', 'TELL', 'ONTX',
#                 'IDEX', 'OCGN', 'BCRX', 'CBAT', 'AKBA', 'TTNP', 'GLBS', 'MVIS', 'MARA', 'VSTM', 'AMRS', 'SNDL',
#                 'HJLI', 'CRBP', 'AXAS', 'CHFS', 'BNGO', 'XSPA', 'TNXP', 'AEZS', 'INPX', 'RIOT', 'GERN', 'SALM',
#                 'AYTU', 'NTEC']
# history_file = Scraper().download_all(symbol_list=symbol_list)
# clusterer = QuantClusterer(path.join(data_dir,'out.csv'))
# clusterer.generate_clusters()
# clusterer.save_groups(path.join(data_dir, 'groups.json'))

trader = ClusterTrader(path.join('./data', 'groups.json'), max_group_size=50)
t = Thread(target=trader.run)
t.start()
t.join()
