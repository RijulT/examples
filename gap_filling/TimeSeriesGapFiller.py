import sys
import os
import pandas as pd
import datetime
import matplotlib.pyplot as plt
import matplotlib.colors as colors
import matplotlib.cm as cm

# Globals to tune
mult=100
div=10
# Only for debugging use
showDist=False

def usage():
    print("Usage: python TimeseriesGapFiller.py [-h] -i inputDir [-o outDir] [-t timeCol] [-v valCol] [-f timeFormat] [-p] [-g gapns]")
    print("where:")
    print("\t-h - OPTIONAL displays these instructions.")
    print("\tinputDir - Path to input directory containing signal files to be processed")
    print("\t\tInput files must be csv format and include a header row.")
    print("\toutputDir - OPTIONAL - Path to output directory where to place files with gaps filled.")
    print("\t\tDefault=Output (subdirectory added to input directory")
    print("\ttimeCol - OPTIONAL the name of the column that contains timestamps.  Default='time'.")
    print("\tvalCol - OPTIONAL the name of the column that contains the values. Default='value'")
    print("\t\tThe files are assumed to only contain values for one timeseries identified by this column.")
    print("\ttimeFormat - the string describing the format of the timestamps. Default='%Y-%m-%d %H:%M:%S'")
    print("\t\tMust be one of the formats supported by Python's datetime.strptime method.")
    print("\t\tSee  https://docs.python.org/3/library/datetime.html#strftime-and-strptime-behavior")
    print("\t\t-p - OPTIONAL creates and saves plots.")
    print("\t\t-g - OPTIONAL gap size to use in nanoseconds.  Skips trying to figure out optimal gap and uses passed value instead.")
    print("Program creates files with the original signal file's rows")
    print("plus additional rows with time and values (interpolated) to fill gaps that prevent Falkonry from evaluating assessments.")
    print("Currently only numeric values are supported.  Hence any non-numeric values will be skipped.")
    print("The output files will be the same as the input names plus '_filled'.")
    print("\tExample: input FILEX.csv will result in output FILEX_filled.csv.")
    sys.exit(1)

def processargs():
    indir = None
    outdir = None
    vcol = "value"
    tcol = "times"
    tformat = "%Y-%m-%d %H:%M:%S"
    plot=False
    gapsize=0
    # Arg processing
    if((len(sys.argv)>1 and "-h" in map(lambda a:a.lower(),sys.argv)) or len(sys.argv)<3 ):
        usage()
    for i in range(1, len(sys.argv), 2):
        arg = sys.argv[i].lower()
        if(i<len(sys.argv)-1):
            val = sys.argv[i + 1]
        if arg == "-i":
            indir = val
        elif arg == "-o":
            outdir = val
        elif arg == "-t":
            tcol = val
        elif arg == "-v":
            vcol = val
        elif arg == "-f":
            tformat = val
        elif arg=="-g":
            try:
                gapsize=int(val)
                if(gapsize<=0):
                    raise ValueError("Value Must be positive and greater than zero")
            except ValueError as ve:
                print("Invalid gap size in nanoseconds")
                print(sys.exc_info()[1])
                usage()
        elif arg =="-p":
            plot=True
        else:
            print("Unknown argument %s" % arg)

    if(not indir):
        print("Input directory not specified")
        usage()
    elif(not os.path.isdir(indir)):
        print("Input directory %s is not valid" % indir)
        sys.exit(1)

    # check tformat
    try:
        nowstr=datetime.datetime.now().strftime(tformat)
    except ValueError:
        print("'%s' is not valid timestamp format." % tformat)
        sys.exit(1)

    return indir,outdir,tcol,vcol,tformat,plot,gapsize

def handleoutputdir(outdir,indir):
    if(not outdir):
        outdir=os.path.join(indir,"Output")
    if(not os.path.exists(outdir)):
        os.mkdir(outdir)
    # Empty sub directory
    for f in os.listdir(outdir):
        if(f.lower().endswith(".csv")):
            os.remove("%s%s%s" % (outdir,os.sep,f))
        if (f.lower().endswith(".pdf")):
            os.remove("%s%s%s" % (outdir, os.sep, f))
    return outdir

# GLOBALS FOR MATPLOTLIB
# Subplots spacing - NOT SURE THESE ARE WORKING
left = 0.125  # the left side of the subplots of the figure
right = 0.9  # the right side of the subplots of the figure
bottom = 0.1  # the bottom of the subplots of the figure
top = 0.9  # the top of the subplots of the figure
wspace = 0.2  # the amount of width reserved for space between subplots,
# expressed as a fraction of the average axis width
hspace = 0.5  # the amount of height reserved for space between subplots,
# expressed as a fraction of the average axis height

# set for different colors using tab20 map
colorMap = plt.get_cmap('tab20')
scalarMap = cm.ScalarMappable(norm=colors.Normalize(1, 20), cmap=colorMap)

# format for date time
from pandas.plotting import register_matplotlib_converters
register_matplotlib_converters()

# Borrowed from another project - make no sense anymore
def plotAndSave(df1,df2,tcol,vcol,f,outdir):
    # Make index out of time
    df1=df1.set_index(tcol)
    df2=df2.set_index(tcol)

    plt.ioff() # force interactive mode off
    plt.rcParams.update({'font.size': 8})

    # title
    title=vcol

    # adjust subplots
    plt.subplots_adjust(left, bottom, right, top, wspace, hspace)

    # create figure wiht 2 subplots
    fig,axes = plt.subplots(2,1,sharex='col') # shared x axis along each column

    # set figure size
    fig.set_figheight(2.3*2) # 2 subplots
    fig.set_figwidth(6)

    axis1=axes[0] # get axis
    axis1.set_title(title) # set plot title
    axis1.set_xlabel(tcol)
    axis1.plot(df1[vcol],color='black', marker='.',linestyle='None')

    axis2=axes[1]
    axis2.set_title(title+" gaps filled") # set plot title
    axis2.set_xlabel(tcol)
    axis2.plot(df2[vcol],color='blue',marker='.',linestyle='None')

    fig.tight_layout()
    fig.savefig("%s%s%s_fig.pdf" % (outdir,os.sep,f[:f.rfind('.')]), dpi=150)
    plt.close(fig)
    fig.clear()


def showHist(series,title,bins,xlabel,ylabel="Count"):
    # df_to_use['dt'].apply(lambda d:d.total_seconds()).hist(label="Before filling gaps")
    fig, ax = plt.subplots()

    # the histogram of the data
    ax.hist(series, bins)
    ax.set_xlabel(xlabel)
    ax.set_ylabel(ylabel)
    ax.set_title(title)
    # Tweak spacing to prevent clipping of ylabel
    fig.tight_layout()
    plt.show()

# get stats in a series of time deltas
# calculates gap size (gap)
def getgapstats(dt):
    med = dt.median()
    gap = mult * med / div
    gaps=dt.loc[lambda dt: dt >= gap]
    mx=gaps.max()
    mi=gaps.min()
    mn=gaps.mean()
    cnt=gaps.shape[0]
    sm=gaps.sum()
    return gap,med,mx,mi,mn,cnt,sm

# fill gaps in the series passed
def replacegaps(dt, gap):
    gaps,newgaps=getgapstofill(dt,gap)
    newdeltas = dt.drop(gaps.index)
    newdeltas = newdeltas.append(newgaps)
    return newdeltas

# returns gap locations and number of intervals to fill
def getgapstofill(dt,gap):
    gaps=dt.loc[lambda dt: dt >= gap]
    # return new gaps
    intervals = pd.Series()
    for g in gaps:
        fullintervals=pd.Series(int(g/gap)*[gap])
        intervals=intervals.append(fullintervals)
        # fraction of a gap
        intervals=intervals.append(pd.Series([g-fullintervals.sum()]))
    return gaps,intervals

maxsearch=5000
def calculategapdelta(df):
    # get series of deltas
    deltas=df['dt']

    # get median and gap count
    gap, med, mx, mn, me, cnt, sm = getgapstats(deltas)
    if(cnt==0):
        print("Nothing to do")
        print("Returning gap=%s" % pd.Timedelta.min)
        return pd.Timedelta.min
    prevcount=cnt
    initmed=med
    prevmed=med
    # search at rate proportional to ...
    delta_mean=deltas.describe()['mean']
    dt_large=max(med,delta_mean)/div
    dt_small=min(med,delta_mean)/div
    dt=dt_large
    print("Initial median to fill gaps=%s" % med)
    gaps,intervals = getgapstofill(deltas, gap)
    print("%s gaps ranging from %s to %s" % (gaps.shape[0],gaps.min(),gaps.max()))
    print("Points required to fill gaps=%s" % intervals.shape[0])
    found=False
    for i in range(maxsearch):
        # fill gaps
        newdeltas = replacegaps(deltas, gap)
        # get new stats
        stats = getgapstats(newdeltas)
        reqmed=stats[1]
        cnt=stats[5]

        # if require is same as prev this is the only solution
        if(reqmed==prevmed):
            print("No need to search after iteration %s" % i)
            print("Maximum median to fill gaps=%s" % prevmed)
            found=True
            break
        # there are many values that results in count of 0
        # we are looking for value where count starts increasing
        # the previous guessed gap is the answer!
        if(cnt>0 and prevcount==0):
            print("Boundary Crossed at iteration %s" % i)
            if(abs(prevmed-med)<=dt_small):
                print("Maximum median to fill gaps=%s" % prevmed)
                found=True
                break
            else:
                print("Backing up and switching to smaller step")
            med=prevmed
            dt=dt/3  # this needs to be tuned
            dt=dt if dt>dt_small else dt_small
            cnt=0
        prevcount=cnt
        prevmed=med
        # increase gap and try again
        med += dt
        gap = mult * med / div
    if(not found):
        print("Max iteration=%s exceeded" % maxsearch)
        print("Using median to fill gaps=%s" % initmed)
    gap= mult * prevmed / div
    gaps,intervals = getgapstofill(deltas, gap)
    print("%s gaps ranging from %s to %s" % (gaps.shape[0],gaps.min(),gaps.max()))
    print("Points required to fill gaps=%s" % intervals.shape[0])
    return gap

def main():
    # process args
    indir,outdir,tcol,vcol,tformat,plot,gapsize=processargs()

    # Handle output directory
    outdir=handleoutputdir(outdir,indir)

    # process files in input directory
    for f in os.listdir(indir):
        if(f.lower().endswith(".csv") or f.lower().endswith(".xlsx")):
            print("Processing file %s%s%s" % (indir,os.sep,f))
            df=pd.DataFrame()
            if(f.lower().endswith(".csv")):
                df=pd.read_csv("%s%s%s" % (indir,os.sep,f))
                # Convert time column to a datetime
                df[tcol]=pd.to_datetime(df[tcol],format=tformat)
            else:
                df=pd.read_excel("%s%s%s" % (indir,os.sep,f))
                vcol=[col for col in df.columns if col!=tcol][0] # assumes is first column not timestamp

            # Convert value column to numeric and drop NaN
            df[vcol]=pd.to_numeric(df[vcol],errors='coerce')
            df_to_use=pd.DataFrame(df.dropna())

            # Sort in order of time and drop duplicates
            df_to_use.sort_values(tcol,inplace=True)
            df_to_use.drop_duplicates(subset=tcol,keep='first',inplace=True)

            # Save cleansed inputs as CSV
            df_to_use.to_csv("%s%s%s_cleansed.csv" % (outdir,os.sep,f[:f.rfind('.')]),index=False)

            # get positive time differences to next row (ns resolution)
            df_to_use['dt']=-1.0*df_to_use[tcol].diff(periods=-1)
            print("Rows in set before filling gaps=%s" % df_to_use.shape[0])

            # Get optimal gap delta, if asked to do so
            maxgap=pd.Timedelta(gapsize,'ns')
            if(gapsize<=0):
                maxgap=calculategapdelta(df_to_use)
            if(maxgap>pd.Timedelta.min):
                if(showDist):
                    showHist(df_to_use['dt'].apply(lambda d:d.total_seconds()),'Before filling gaps',20,'Interval (sec)')

                print("Using delta of %s to fill gaps" % maxgap)

                # get rows and next where dt is >= maxgap
                to_interp=df_to_use.loc[lambda df: df.dt >= maxgap]
                to_interp_next=df_to_use.loc[lambda df: df.shift(1)[tcol].isin(to_interp[tcol])][[tcol,vcol]]

                # loop through gaps filling them and adding to original df as well
                nextrows=to_interp_next.iterrows()
                for index,row in to_interp.iterrows():
                    nextrow=next(nextrows)[1]
                    # compute slope and initialize next timestamp
                    slope=(nextrow[vcol]-row[vcol])/(nextrow[tcol].value-row[tcol].value)
                    nextts = row[tcol] + maxgap - pd.Timedelta(value=1, units="ns") # minus 1 ns to allow for precision
                    while(nextts<nextrow[tcol]):
                        interp_val=slope*(nextts.value-row[tcol].value)+row[vcol]
                        # create dictionary of entry to add to dataframe
                        entry={}
                        for col in df_to_use.columns:
                            if(col==tcol):
                                entry[col]=nextts
                            elif(col==vcol):
                                entry[col]=interp_val
                            else:
                                entry[col]=row[col]
                        # add new values
                        df_to_use=df_to_use.append(entry,ignore_index=True)
                        # update next time
                        nextts+=maxgap-pd.Timedelta(value=1,units="ns") # minus 1 ns to allow for precision

                print("Row in set after filling gaps=%s" % df_to_use.shape[0])

                # sort in time order
                df_to_use.sort_values(tcol,inplace=True)

                # check results after filling gaps
                df_to_use['dt']=-1.0*df_to_use[tcol].diff(periods=-1)
                print("Gap used to fill gaps=%s (%s s)" % (maxgap,maxgap.total_seconds()))
                print("Gaps left=%s" % df_to_use['dt'].loc[lambda dt: dt >= maxgap].shape[0])
                gap=getgapstats(df_to_use['dt'])[0]
                print("New gap required to fill gaps=%s" % gap)
                if(maxgap>gap):
                    print("Something went wrong with file %s%s%s - required gap is smaller that used gap" % (indir,os.sep,f))
                    print("Gaps left=%s" % df_to_use['dt'].loc[lambda dt: dt >= gap].shape[0])

                if(showDist):
                    showHist(df_to_use['dt'].apply(lambda d:d.total_seconds()),'After filling gaps',20,'Interval (sec)')

                # Save output file
                df_to_use[[col for col in df_to_use.columns if col!='dt']].to_csv("%s%s%s_filled.csv" % (outdir,os.sep,f[:f.rfind('.')]),index=False,date_format="%Y-%m-%d %H:%M:%S.%f")

                # Plot and save before and after
                if(plot):
                    plotAndSave(df,df_to_use,tcol,vcol,f,outdir)
            else:
                print("No gaps to fill on file %s%s%s" % (indir,os.sep,f))

if __name__ == "__main__":
    main()
