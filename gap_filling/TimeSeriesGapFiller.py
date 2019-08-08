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
