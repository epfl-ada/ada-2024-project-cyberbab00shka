import plotly.graph_objects as go
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns

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
    percentiles = np.quantile(data[col], [i/num_bins for i in range(num_bins + 1)])
    # adjust them a little bit
    percentiles[0] -= 0.01
    percentiles[-1] += 0.01
    # discretize the data
    data[col + "_bin"] = np.digitize(data[col], percentiles)
    col = col + "_bin"
    # change the 
    ticks_name = [f"[{percentiles[i - 1]:.2f}, {percentiles[i]:.2f}]" for i in range(1, len(percentiles))]
    data[col] = data[col].apply(lambda x: ticks_name[x - 1])
    return col, data

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
    # drop the rows with missing values
    data_part = data[[xcol, ycol]].dropna()
    print(data_part.shape)
    
    # if the data is not categorical, then we need to discretize it
    if data_part[xcol].dtype != "object":
        xcol, data_part = find_percentiles(data_part, xcol, num_xbins)
    if data_part[ycol].dtype != "object":
        ycol, data_part = find_percentiles(data_part, ycol, num_ybins)
    print(data_part.shape)

    # if the number of unique values is too high, then we need to drop some of them
    if len(data_part[xcol].unique()) > num_xbins + 1:
        xcol, data_part = add_column_other(data_part, xcol, num_xbins)
    if len(data_part[ycol].unique()) > num_ybins + 1:
        ycol, data_part = add_column_other(data_part, ycol, num_ybins)
    print(data_part.shape)

    xticks_name = data_part[xcol].unique()
    yticks_name = data_part[ycol].unique()
    
    # calculate the appearance of each bin
    occurences = data_part[[xcol, ycol]].value_counts().reset_index(name="count")
    label2index_x = {label: i for i, label in enumerate(data_part[xcol].unique())}
    label2index_y = {label: i for i, label in enumerate(data_part[ycol].unique())}

    # fill the grid with those values
    grid = np.zeros((len(label2index_y), len(label2index_x)), dtype=float)
    for i, row in occurences.iterrows():
        grid[label2index_y[row[ycol]], label2index_x[row[xcol]]] = row["count"]
    
    # maybe normalize the grid
    # (this is a little bit tricky)
    if normalize == "first":
        # normalize each column
        ret_grid = grid / grid.sum(axis=0).reshape(1, -1)
    elif normalize == "second":
        # normalize each row
        ret_grid = grid / grid.sum(axis=1).reshape(-1, 1)
    elif normalize == "both":
        ret_grid = grid / grid.sum()
    else:
        ret_grid = grid

    if compare_default_value != "none":
        if normalize == "first":
            # normalize column which is sum of rows
            second_part = normalize_1d(grid.sum(axis=1)).reshape(-1, 1)
        elif normalize == "second":
            # normalize row which is sum of colums
            second_part = normalize_1d(grid.sum(axis=0)).reshape(1, -1)

        if compare_default_value == "divide":
            ret_grid = ret_grid / second_part
        elif compare_default_value == "subtract":
            ret_grid = ret_grid - second_part
        else:
            raise RuntimeError("Unknown value for compare_default_value")

    return ret_grid, xticks_name, yticks_name, label2index_x, label2index_y, [occurences]


def histogram_3d_plotly(data, xcol, ycol, title, num_xbins=10, num_ybins=10, normalize="first", compare_default_value="none", gap=0.01):
    '''
    # Example of usage:
    fig = histogram_3d_plotly(data, "race", "archetype", "test", normalize="y", gap=0.1)
    fig.update_layout(
        width=800,
        height=800,
    )
    fig.show()
    '''
    grid, xticks_name, yticks_name, label2index_x, label2index_y = \
        calculate_ticks_and_norm(
            data=data, 
            xcol=xcol, ycol=ycol, 
            num_xbins=num_xbins, num_ybins=num_ybins, 
            normalize=normalize, compare_default_value=compare_default_value
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

def plot_2d_heatmap(data, xcol, ycol, num_xbins=10, num_ybins=10, normalize="first", compare_default_value="none", percentage=False):
    grid, xticks_name, yticks_name, label2index_x, label2index_y, to_debug = calculate_ticks_and_norm(
        data=data, 
        xcol=xcol, ycol=ycol, 
        num_xbins=num_xbins, num_ybins=num_ybins, 
        normalize=normalize,
        compare_default_value=compare_default_value
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
    sns.heatmap(
        grid, 
        xticklabels=xticks_name, 
        yticklabels=yticks_name,
        center=center,
        annot=True
    )

    return grid, to_debug
