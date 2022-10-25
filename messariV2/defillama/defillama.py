"""This module is meant to contain the DeFiLlama class"""

# Global imports
import datetime
from string import Template
from typing import Union, List, Dict

import pandas as pd

from messari.dataloader import DataLoader
# Local imports
from messari.utils import validate_input, get_taxonomy_dict, time_filter_df
from .helpers import format_df

##########################
# URL Endpoints
##########################
DL_PROTOCOLS_URL = "https://api.llama.fi/protocols"
DL_GLOBAL_TVL_URL = "https://api.llama.fi/charts/"
DL_CURRENT_PROTOCOL_TVL_URL = Template("https://api.llama.fi/tvl/$slug")
DL_CHAIN_TVL_URL = Template("https://api.llama.fi/charts/$chain")
DL_GET_PROTOCOL_TVL_URL = Template("https://api.llama.fi/protocol/$slug")
DL_CHAINS_URL = "https://api.llama.fi/chains/"


class DeFiLlama(DataLoader):
    """This class is a wrapper around the DeFi Llama API
    """

    def __init__(self):
        messari_to_dl_dict = get_taxonomy_dict("messari_to_dl.json")
        DataLoader.__init__(self, api_dict=None, taxonomy_dict=messari_to_dl_dict)

    def get_protocol_tvl_timeseries(self, asset_slugs: Union[str, List],
                                    start_date: Union[str, datetime.datetime] = None,
                                    end_date: Union[str, datetime.datetime] = None) -> pd.DataFrame:
        """Returns times TVL of a protocol with token amounts as a pandas DataFrame.
        Returned DataFrame is indexed by df[protocol][chain][asset].
        Parameters
        ----------
           asset_slugs: str, list
               Single asset slug string or list of asset slugs (i.e. bitcoin)
           start_date: str, datetime.datetime
               Optional start date to set filter for tvl timeseries ("YYYY-MM-DD")
           end_date: str, datetime.datetime
               Optional end date to set filter for tvl timeseries ("YYYY-MM-DD")
        Returns
        -------
           DataFrame
               pandas DataFrame of protocol TVL, indexed by df[protocol][chain][asset]
               to look at total tvl across all chains, index with chain='all'
               to look at total tvl across all tokens of a chain, asset='totalLiquidityUSD'
               tokens can be indexed by asset='tokenName' or by asset='tokenName_usd'
        """
        slugs = self.translate(asset_slugs)

        slug_df_list: List = []
        for slug in slugs:
            endpoint_url = DL_GET_PROTOCOL_TVL_URL.substitute(slug=slug)
            protocol = self.get_response(endpoint_url)
        return protocol
    
    def get_global_tvl_timeseries(self, start_date: Union[str, datetime.datetime] = None,
                                  end_date: Union[str, datetime.datetime] = None) -> pd.DataFrame:
        """Returns timeseries TVL from total of all Defi Llama supported protocols
        Parameters
        ----------
           start_date: str, datetime.datetime
               Optional start date to set filter for tvl timeseries ("YYYY-MM-DD")
           end_date: str, datetime.datetime
               Optional end date to set filter for tvl timeseries ("YYYY-MM-DD")
        Returns
        -------
           DataFrame
               DataFrame containing timeseries tvl data for every protocol
        """
        global_tvl = self.get_response(DL_GLOBAL_TVL_URL)
        global_tvl_df = pd.DataFrame(global_tvl)
        global_tvl_df = format_df(global_tvl_df)
        global_tvl_df = time_filter_df(global_tvl_df, start_date=start_date, end_date=end_date)
        return global_tvl_df

    def get_chain_tvl_timeseries(self, chains_in: Union[str, List],
                                 start_date: Union[str, datetime.datetime] = None,
                                 end_date: Union[str, datetime.datetime] = None) -> pd.DataFrame:
        """Retrive timeseries TVL for a given chain
        Parameters
        ----------
           chains_in: str, list
               Single asset slug string or list of asset slugs (i.e. bitcoin)
           start_date: str, datetime.datetime
               Optional start date to set filter for tvl timeseries ("YYYY-MM-DD")
           end_date: str, datetime.datetime
               Optional end date to set filter for tvl timeseries ("YYYY-MM-DD")
        Returns
        -------
           DataFrame
               DataFrame containing timeseries tvl data for each chain
        """
        chains = validate_input(chains_in)

        chain_df_list = []
        for chain in chains:
            endpoint_url = DL_CHAIN_TVL_URL.substitute(chain=chain)
            response = self.get_response(endpoint_url)
            chain_df = pd.DataFrame(response)
            chain_df = format_df(chain_df)
            chain_df_list.append(chain_df)

        # Join DataFrames from each chain & return
        chains_df = pd.concat(chain_df_list, axis=1)

        # If chains_df is empty, return an empty DataFrame
        if chains_df.empty:
            return pd.DataFrame()

        chains_df.columns = chains
        chains_df = time_filter_df(chains_df, start_date=start_date, end_date=end_date)
        return chains_df

    def get_current_tvl(self, asset_slugs: Union[str, List]) -> Dict:
        """Retrive current protocol tvl for an asset
        Parameters
        ----------
           asset_slugs: str, list
               Single asset slug string or list of asset slugs (i.e. bitcoin)
        Returns
        -------
           DataFrame
               Pandas Series for tvl indexed by each slug {slug: tvl, ...}
        """
        slugs = validate_input(asset_slugs)

        tvl_dict = {}
        for slug in slugs:
            endpoint_url = DL_CURRENT_PROTOCOL_TVL_URL.substitute(slug=slug)
            tvl = self.get_response(endpoint_url)
            if isinstance(tvl, float):
                tvl_dict[slug] = tvl
            else:
                print(f"ERROR: slug={slug}, MESSAGE: {tvl['message']}")

        tvl_series = pd.Series(tvl_dict)
        tvl_df = tvl_series.to_frame("tvl")
        return tvl_df

    def get_protocols(self) -> pd.DataFrame:
        """Returns basic information on all listed protocols, their current TVL
        and the changes to it in the last hour/day/week
        Returns
        -------
        DataFrame
           DataFrame with one column per DeFi Llama supported protocol
        """
        protocols = self.get_response(DL_PROTOCOLS_URL)

        protocol_dict = {}
        for protocol in protocols:
            protocol_dict[protocol["slug"]] = protocol

        protocols_df = pd.DataFrame(protocol_dict)
        return protocols_df

    def get_chains(self) -> List[str]:
        """Get the names of all chains supported by Defi Llama
        Used downstream to get the names/TVL of protocols on each chain
        Returns
        -------
        List
            List of chain name strings
        """
        chains = self.get_response(DL_CHAINS_URL)

        chain_names = [chain['name'] for chain in chains]

        # Sort chain name results to ensure consistent order
        chain_names = sorted(chain_names)

        return chain_names
