from . import rawdata_readlines_method as readstring
from . import rawdata_pd_read_fwf_method as readfwf
#from . import rawdata_read_numpy as readnp
from . import rawdata_newread as new
from . import rawdata_newread_multiprocessing2 as new2

from .merge import mergedf, mergedf_multiprocessing
if __name__ == '__main__':
    #readstring.get_hsccit(2018, month='12') #3.25s #2.61s
    #readstring.get_hscoit(2018, month='12')
    #readstring.get_hscoccit(2018, month='12')
    #readfwf.get_hsccit(2018, month='12') #4.66s
    #readnp.get_hsccit(2018, month='12') #3.71s not finished
    new.get_hsccit(2018, month='12')#1.64s #742ms
    #new2.get_hsccit(2018, month='12')#1.64s #742ms

    #new.get_hscoit(2018, month='12')
    #new.get_hscoccit(2018, month='12')
    #mergedf(startyear=2016, endperiod=201907, type="hsccit") #14.24s #13.96 #11.06s
    #mergedf_multiprocessing(startyear=2016, endperiod=201907, type="hsccit") #8s #8.47
