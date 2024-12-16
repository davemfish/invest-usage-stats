import marimo

__generated_with = "0.10.2"
app = marimo.App(width="full")


@app.cell
def _():
    # Export from a gcloud shell.
    # It does not include all columns in the table because a dump of all columns
    # yielded a bad csv probably because of this known issue with exporting NULL values:
    # https://cloud.google.com/sql/docs/mysql/known-issues#import-export

    # gcloud sql export csv --escape="5C" software-usage-stats gs://logging-natcap/model_log_table-2024-12-16.csv --query="SELECT 'model_name', 'invest_release', 'invest_interface', 'system_full_platform_string', 'time' UNION SELECT model_name, invest_release, invest_interface, system_full_platform_string, time FROM model_log_table" --database=invest_model_usage

    # From a local shell:
    # gsutil cp gs://logging-natcap/model_log_table-2024-12-16.csv .
    return


@app.cell
def _():
    # '%load_ext autoreload\n%autoreload 2' command supported automatically in marimo
    return


@app.cell
def _():
    import pandas
    import altair

    from load_and_clean import load_and_clean_csv

    pandas.set_option('display.max_rows', 100)
    return altair, load_and_clean_csv, pandas


@app.cell
def _():
    table_path = 'model_log_table-2024-12-16.csv'
    return (table_path,)


@app.cell
def _(load_and_clean_csv, table_path):
    df = load_and_clean_csv(table_path)
    # dataframe = mo.ui.dataframe(df)
    # dataframe
    return (df,)


@app.cell
def _(mo):
    mo.md(
        r"""
        # Monthly counts by model
        #### https://storage.googleapis.com/data.naturalcapitalproject.org/invest-usage-stats/invest_monthly_counts_2024-12-16.csv
        """
    )
    return


@app.cell
def _(df, pandas):
    # Aggregate to a time frequency so we can count runs per unit of time per model
    frequency = 'M'
    data = df.groupby([
        pandas.Grouper(key='datetime', freq=frequency),
        pandas.Grouper(key='model')]).size().reset_index(name='counts')
    data['year'] = data.datetime.dt.year
    data['month'] = data.datetime.dt.month
    export = data[['model', 'counts', 'year', 'month']]
    export.to_csv('invest_monthly_counts_2024-12-16.csv', index=False)
    return data, export, frequency


@app.cell
def _(data, frequency, pandas):
    # For the benefit of plots, fill in 0s where no models were run
    wide = data.pivot(index='datetime', columns='model', values='counts')
    wide.fillna(0, inplace=True)
    months_with_counts = len(wide)

    # And in case there were months where no models were run
    # And in case the first & last months are incomplete (assume they are)
    # trim them off with offsets
    begin = wide.index.min() + pandas.offsets.MonthBegin()
    end = wide.index.max() - pandas.offsets.MonthEnd()
    date_range = pandas.date_range(begin, end, freq=frequency)
    wide = wide.reindex(date_range, fill_value=0)
    print(f'complete data from {begin} to {end}')
    return begin, date_range, end, months_with_counts, wide


@app.cell
def _(pandas, wide):
    wide_1 = wide.reset_index()
    long = pandas.melt(wide_1, id_vars='index')
    return long, wide_1


@app.cell
def _(altair, long):
    def plot_model_counts_over_time(model_list, title=None):
        altair.data_transformers.disable_max_rows()
        selection = altair.selection_multi(fields=['model'], bind='legend')

        to_plot = long[long['model'].isin(model_list)]

        return (
            altair.Chart(to_plot).mark_line().encode(
                altair.X('index:T', axis=altair.Axis(format='%Y-%m'), title=None),
                altair.Y('value:Q', title='runs per month'),
                color=altair.Color('model', scale=altair.Scale(scheme='category10')),
                opacity=altair.condition(selection, altair.value(1), altair.value(0.2)),
                size=altair.value(1)
            ).properties(
                width=800,
                height=300,
                title=title
            ).add_selection(
                selection
            ).configure_axis(
                grid=False,
                labelFontSize=12,
                titleFontSize=14,
                titlePadding=15
            ).configure_legend(
                labelFontSize=12,
                title=None,
            )
        )
    return (plot_model_counts_over_time,)


@app.cell
def _(altair, long):
    all_models_counts = long.groupby('index').sum().reset_index()
    altair.Chart(all_models_counts).mark_line().encode(
        altair.X('index:T', axis=altair.Axis(format='%Y-%m'), title=None),
        altair.Y('value:Q', title='runs per month'),
        size=altair.value(1)
    ).properties(
        width=800,
        height=300,
        title='InVEST runs per month'
    ).configure_axis(
        grid=False
    )
    return (all_models_counts,)


@app.cell
def _(mo):
    mo.md(r"""## These plots are interactive - click a series in the legend""")
    return


@app.cell
def _(data):
    deprecated_models = [
        'fisheries',
        'scenario_generator',
        'fisheries_hst',
        'finfish_aquaculture',
        'globio',
        'habitat_suitability',
        'marine_water_quality_biophysical',
        'overlap_analysis',
        'timber',
    ]
    high_use_models = []
    low_use_models = []
    model_list = [
        model for model in data['model'].unique() if model not in deprecated_models]
    for model in model_list:
        d = data.loc[data['model'] == model]
        # print(d['counts'])
        if any(d['counts'] > 600):
            high_use_models.append(model)
        else:
            low_use_models.append(model)
    # print(high_use_models)
    # print(low_use_models)
    return (
        d,
        deprecated_models,
        high_use_models,
        low_use_models,
        model,
        model_list,
    )


@app.cell
def _(high_use_models, plot_model_counts_over_time):
    plot_model_counts_over_time(high_use_models, 'high-use models (over 600 runs in a month)')
    return


@app.cell
def _(low_use_models, plot_model_counts_over_time):
    plot_model_counts_over_time(low_use_models, 'low-use models (less than 600 runs in a month)')
    return


@app.cell
def _():
    import marimo as mo
    return (mo,)


if __name__ == "__main__":
    app.run()
