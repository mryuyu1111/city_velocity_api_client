#!/usr/bin/env python

import api_proxy

'''
Testing playground
'''
if __name__ == "__main__":
    vc = api_proxy.Velocity_Client()
    metadata_tags = ['C_INDEX.WORLD.YB_WGBI.YB_WGBI_ORIGINAL.YB_ALL.YB_ALL.YB_ALL.YB_ALL.IDXVALUE.LOCAL', # locked
                     'FX.SPOT.USD.CNH.CITI',
                     'FX.FLOWS.DEVELOPED.FXF_G10.FXF_ALL.FXF_BANK.NET_INDEX', #locked
                     'EM.FX.SPOT.USD.AED.CITI', # emerging markets
                     'RATES.MBS.CMM.CMM100.CMM_SPREAD']
    vc.retrieve_metadata(metadata_tags)

    tag_browser_tags = ['',  # root
                     'COMMODITIES.SPOT',
                     'COMMODITIES.SPOT.SPOT_GOLD',
                     'FX.FLOWS'] # locked
    for prefix in tag_browser_tags:
        vc.retrieve_tag_browser(prefix)

    tag_list_tags = ['COMMODITIES.SPOT',
                     'COMMODITIES.SPOT.SPOT_GOLD',
                     'FX.FLOWS']  # locked
    for prefix in tag_list_tags:
        vc.retrieve_tag_list(prefix)