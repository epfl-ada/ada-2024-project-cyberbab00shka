import plotly.graph_objects as go
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
import scipy.stats as sps

def draw_3d_rectangle(min_corner, max_corner, color='blue', opacity=0.5):
    # Unpack the corners
    x_min, y_min, z_min = min_corner
    x_max, y_max, z_max = max_corner

    # Define the vertices of the rectangular prism
    x = [x_min, x_max, x_max, x_min, x_min, x_max, x_max, x_min]
    y = [y_min, y_min, y_max, y_max, y_min, y_min, y_max, y_max]
    z = [z_min, z_min, z_min, z_min, z_max, z_max, z_max, z_max]

    # Define the faces using vertex indices
    triangles = [
        [0, 1, 3], [1, 2, 3],  # Bottom
        [4, 5, 7], [5, 6, 7],  # Top
        [0, 1, 4], [1, 5, 4],  # Front
        [1, 2, 5], [2, 6, 5],  # Right
        [2, 3, 6], [3, 7, 6],  # Back
        [3, 0, 7], [0, 4, 7]   # Left
    ]
    i = [el[0] for el in triangles]
    j = [el[1] for el in triangles]
    k = [el[2] for el in triangles]

    # Create the Mesh3d object
    return go.Mesh3d(
        x=x, y=y, z=z,
        i=i, j=j, k=k,
        color=color,
        opacity=opacity
    )


def normalize_1d(arr):
    return arr/(arr.sum() + 1e-7)


def find_percentiles(data, col, num_bins):
    '''
    When data is continuous, we need to discretize it into bins.
    '''
    # calculate the percentiles
    data_no_nan = data[[col]].dropna()
    percentiles = np.quantile(data_no_nan[col], [i/num_bins for i in range(num_bins + 1)])
    # adjust them a little bit
    percentiles[0] -= 0.01
    percentiles[-1] += 0.01
    # discretize the data
    col_bin = col + "_bin"
    data_no_nan[col_bin] = np.digitize(data_no_nan[col], percentiles)
    # change the ticks name
    ticks_name = [f"[{percentiles[i - 1]:.2f}, {percentiles[i]:.2f}]" for i in range(1, len(percentiles))]
    data_no_nan[col_bin] = data_no_nan[col_bin].apply(lambda x: ticks_name[x - 1])


    data[col_bin] = data[col].astype(str)
    data.loc[~data[col].isna(), col_bin] = data_no_nan[col_bin]
    data.loc[data[col].isna(), col_bin] = "Other"

    return col_bin, data


def remove_nans(data, col):
    data.loc[data[col].isna(), col] = "Other"
    return col, data
    
def ttest_bernoulli_ind(
        theta1, theta2, 
        num1, num2,
        mht=False, alpha=0.05
    ):
    ''' 
    theta1: shape (n1, n2) probabilities
    theta2: shape (n1, n2) probabilities
    num1: shape (n1, n2) of numbers of samples for the first group
    num2: shape (n1, n2) of numbers of samples for the second group
    '''

    std = np.sqrt(theta1 * (1 - theta1)/num1 + theta2 * (1 - theta2)/num2)
    ZZZ_score = (theta1 - theta2)/std
    p_value = 2 * sps.norm.sf(np.absolute(ZZZ_score))
    if mht:
        p_value = np.minimum(1.0, p_value * num1.size)
    return (p_value <= alpha)


def add_column_other(data, col, num_bins):
    '''
    When the number of columns is too high then we need to drop some values
    and write them as "Other"
    '''
    # calculate the appearance of each value and sort by decreasing order
    vc = data[col].value_counts().reset_index().sort_values(by="count", ascending=False)
    # take top and bottom values
    top_num = set(vc[col].values[:num_bins])
    bottom_num = set(vc[col].values[num_bins:])
    # create a function that will rename the values
    # it might be faster to use a the first or the second
    if len(top_num) < len(bottom_num):
        rename_to_other = lambda x: x if x in top_num else "Other"
    else:
        rename_to_other = lambda x: x if not x in bottom_num else "Other"
    # rename the values
    data[col + "_other"] = data[col].apply(rename_to_other)
    col = col + "_other"
    return col, data


def calculate_ticks_and_norm(
        data, 
        xcol, ycol, 
        num_xbins=10, num_ybins=10, 
        normalize="first", 
        compare_default_value="none",
        alpha=0.05,
        mht=False,
        do_not_show_x=None,
):
    '''

    parameters:
    - xcol: str 
        which column would be on x axis
    - ycol: str 
        which column would be on y axis
    - num_xbins: int=10
        number of bins for x axis
    - num_ybins: int=10 
        number of bins for y axis
    - normalize: "first" | "second" | "both" | "none"="first", 
        how to normalize the data
    - compare_default_value: "none" | "divide" | "subtract" = "none
        We want to look at how "abnormal" the data is if we look at the distribution with the fixed ycol or xcol
        If True, then we will normalize the data by the sum of the non-normalized column
    '''
    if do_not_show_x is None:
        do_not_show_x = []
    do_not_show_x = set(do_not_show_x)
    # drop the rows with missing values
    data_part = data[[xcol, ycol]].copy()
    
    # if the data is not categorical, then we need to discretize it
    if data_part[xcol].dtype != "object":
        xcol, data_part = find_percentiles(data_part, xcol, num_xbins)
    else:
        xcol, data_part = remove_nans(data_part, xcol)

    if data_part[ycol].dtype != "object":
        ycol, data_part = find_percentiles(data_part, ycol, num_ybins)
    else:
        ycol, data_part = remove_nans(data_part, ycol)

    # if the number of unique values is too high, then we need to drop some of them
    if len(data_part[xcol].unique()) > num_xbins + 1:
        xcol, data_part = add_column_other(data_part, xcol, num_xbins)
    if len(data_part[ycol].unique()) > num_ybins + 1:
        ycol, data_part = add_column_other(data_part, ycol, num_ybins)

    xticks_name = sorted(data_part[xcol].unique().tolist())
    xticks_ids_take = [i for i, x in enumerate(xticks_name) if x not in do_not_show_x]
    yticks_name = sorted(data_part[ycol].unique().tolist())
    
    # calculate the appearance of each bin
    occurences = data_part[[xcol, ycol]].value_counts().reset_index(name="cnt")
    label2index_x = {label: i for i, label in enumerate(xticks_name)}
    label2index_y = {label: i for i, label in enumerate(yticks_name)}

    # fill the grid with those values
    grid = np.zeros((len(label2index_y), len(label2index_x)), dtype=float)
    for i, row in occurences.iterrows():
        grid[label2index_y[row[ycol]], label2index_x[row[xcol]]] = row["cnt"]
    
    # maybe normalize the grid
    # (this is a little bit tricky)
    if normalize == "first":
        # normalize each column
        first_part = grid / grid.sum(axis=0).reshape(1, -1)
    elif normalize == "second":
        # normalize each row
        first_part = grid / grid.sum(axis=1).reshape(-1, 1)
    elif normalize == "both":
        first_part = grid / grid.sum()
    else:
        first_part = grid

    # compare the results with the original distribution
    if compare_default_value != "none":
        if normalize == "first":
            # normalize column which is sum of rows
            second_part = normalize_1d(grid.sum(axis=1)).reshape(-1, 1)
        elif normalize == "second":
            # normalize row which is sum of colums
            second_part = normalize_1d(grid.sum(axis=0)).reshape(1, -1)

        if compare_default_value == "divide":
            ret_grid = first_part / second_part
        elif compare_default_value == "subtract":
            ret_grid = first_part - second_part
        else:
            raise RuntimeError("Unknown value for compare_default_value")
    else:
        ret_grid = first_part

    # calculate statistical meaningfulness
    if normalize == "first" and compare_default_value != "none":
        num1 = grid.sum(axis=0).reshape(1, -1)
        num2 = grid.sum()
        ttest_result = ttest_bernoulli_ind(first_part, second_part, num1, num2, alpha=alpha, mht=mht)
    elif normalize == "second" and compare_default_value != "none":
        num1 = grid.sum(axis=1).reshape(-1, 1)
        num2 = grid.sum()
        ttest_result = ttest_bernoulli_ind(first_part, second_part, num1, num2, alpha=alpha, mht=mht)
    else:
        ttest_result = None

    return ret_grid[:, xticks_ids_take], \
            ttest_result[:, xticks_ids_take] if not ttest_result is None else None, \
            [x for x in xticks_name if not x in do_not_show_x], \
            yticks_name, \
            label2index_x, label2index_y, \
            [occurences, grid]


def histogram_3d_plotly(
        data, 
        xcol, ycol, title, 
        num_xbins=10, num_ybins=10, 
        normalize="first", 
        compare_default_value="none", 
        gap=0.01,
        alpha=0.05,
        mht=False,
        do_not_show_x=None,
    ):
    '''
    # Example of usage:
    fig = histogram_3d_plotly(data, "race", "archetype", "test", normalize="y", gap=0.1)
    fig.update_layout(
        width=800,
        height=800,
    )
    fig.show()
    '''
    grid, xticks_name, yticks_name, label2index_x, label2index_y, _ = \
        calculate_ticks_and_norm(
            data=data, 
            xcol=xcol, ycol=ycol, 
            num_xbins=num_xbins, num_ybins=num_ybins, 
            normalize=normalize, compare_default_value=compare_default_value,
            alpha=alpha,
            mht=mht,
            do_not_show_x=do_not_show_x,
        )

    # create the histogram
    fig = go.Figure()
    for i in range(num_xbins):
        for j in range(num_ybins):
            fig.add_trace(draw_3d_rectangle(
                (i + gap/2, j + gap/2, 0),
                (i + 1 - gap/2, j + 1 - gap/2, grid[i, j]),
                color='blue',
                opacity=1.0
            ))
    # add titles
    fig.update_layout(
        scene=dict(
            xaxis=dict(
                ticktext=xticks_name,
                tickvals=[i + 0.5 for i in range(len(label2index_x))],
                tickmode="array",
                title=xcol
            ),
            yaxis=dict(
                ticktext=yticks_name,
                tickvals=[i + 0.5 for i in range(len(label2index_y))],
                tickmode="array",
                title=ycol
            ),
        ),
        title=dict(
            text=title,
            x=0.5
        )
    )
    return fig

def process_ticks(ticks, max_length=20):
    '''
    If the length of the string is too long, then we need to cut it
    '''
    result = []
    for tick in ticks:
        if len(tick) > max_length:
            tick = tick[:max_length - 3] + "..."
        result.append(tick)
    return result

def plot_2d_heatmap(
        data, 
        xcol, ycol, 
        num_xbins=10, num_ybins=10, 
        normalize="first", compare_default_value="none", 
        percentage=False,
        alpha=0.05,
        mht=False,
        do_not_show_x=None,
    ):
    grid, ttest_result, xticks_name, yticks_name, label2index_x, label2index_y, to_debug = calculate_ticks_and_norm(
        data=data, 
        xcol=xcol, ycol=ycol, 
        num_xbins=num_xbins, num_ybins=num_ybins, 
        normalize=normalize,
        compare_default_value=compare_default_value,
        alpha=alpha,
        mht=mht,
        do_not_show_x=do_not_show_x,
    )

    if compare_default_value == "none":
        center = None
    elif compare_default_value == "divide":
        center = 1
    elif compare_default_value == "subtract":
        center = 0
    else:
        raise RuntimeError("Unknown value for compare_default_value")

    if percentage:
        grid = grid * 100

    if ttest_result is not None:
        grid[~ttest_result] = 0.0

    annot_labels = grid.copy().tolist()
    for i in range(len(annot_labels)):
        for j in range(len(annot_labels[i])):
            if ttest_result is not None and not ttest_result[i, j]:
                annot_labels[i][j] = "NSS"
            else:
                annot_labels[i][j] = f"{annot_labels[i][j]:.2f}"

    sns.heatmap(
        grid, 
        xticklabels=process_ticks(xticks_name), 
        yticklabels=process_ticks(yticks_name),
        center=center,
        annot=annot_labels,
        fmt="",
    )

    return grid, to_debug
