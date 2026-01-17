import cdsapi
import xarray as xr

dataset = "sis-water-level-change-indicators-cmip6"
request = {
    "variable": [
        "mean_sea_level",
        "annual_mean_of_highest_high_water",
        "annual_mean_of_lowest_low_water"
    ],
    "derived_variable": ["absolute_value"],
    "experiment": ["historical"],
    "period": [
        "1997",
        "1998",
        "1999",
        "2000",
        "2001",
        "2002",
        "2003",
        "2004",
        "2005",
        "2006",
        "2007",
        "2008",
        "2009",
        "2010",
        "2011",
        "2012",
        "2013",
        "2014"
    ]
}

# client = cdsapi.Client()
# client.retrieve(dataset, request).download()

ds = xr.open_dataset("e277dd8ebc20760b0d076e27fe2c3be0/historical_tide_actual-value_2014_MSL_v1.nc")
df = ds['MSL'].to_dataframe().reset_index()
df.head(5000).to_csv("msl_preview.csv", index=False)

print(ds)
print(ds.data_vars)
print(ds.coords)