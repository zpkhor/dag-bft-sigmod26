#!/usr/bin/env python3
"""Plot TPS/migration timeline from pasted log blocks.

Paste the LB run log into CSV_BLOCK. Optionally paste a baseline (no-LB) run's
tps_timeline.py output into CSV_BLOCK_BASELINE and pass --baseline to overlay
it as a reference line on the same TPS panel.

Example:
    python benchmark/exp/parse_and_plot_tps_migration_timeline.py \
        --label n4_v_rate_imb60 -o timeline.png

    python benchmark/exp/parse_and_plot_tps_migration_timeline.py \
        --label n4_v_rate_imb60 --baseline -o timeline_vs_baseline.png
"""
import argparse
import csv
import re
from pathlib import Path

import numpy as np
np.Inf = np.inf  # patch for matplotlib compatibility with NumPy 2.0
import matplotlib.pyplot as plt
import matplotlib.lines as mlines
import matplotlib.ticker as ticker
from matplotlib.ticker import MaxNLocator
from plot_lb_configs import (
    TPS_OUTLIER,
    _smoothed_series,
    load_run,
)

# Local x-axis trim (independent of plot_lb_configs.py — tuned for hotspot timeline).
TRIM_LEFT_DUR  = 20
TRIM_RIGHT_DUR = 520

# Paper palette — teal, coral, blue, orange (order matches plot_paper_configs.py)
COLORS = ["#2A9D8F", "#E76F51", "#1f77b4", "#ff7f0e"]


def _apply_sigmod_style():
    plt.rcParams.update({
        "font.family":       "sans-serif",
        "font.size":         8,
        "axes.titlesize":    8,
        "axes.labelsize":    8,
        "xtick.labelsize":   7,
        "ytick.labelsize":   7,
        "legend.fontsize":   7,
        "lines.linewidth":   1.0,
        "axes.linewidth":    0.6,
        "grid.linewidth":    0.4,
        "xtick.major.width": 0.6,
        "ytick.major.width": 0.6,
        "pdf.fonttype":      42,
        "ps.fonttype":       42,
        "grid.linestyle":    "--",
        "grid.color":        "lightgray",
        "grid.alpha":        0.8,
    })

# Hardcoded phase markers for the fig. 3 timeline experiments:
# warmup end at 240s and hotspot shift at 600s.
IMBALANCE_MARKERS_S = (220.0, 580.0)

CSV_BLOCK = """
timestamp_s,tps,n_batches,n_txs,lat_mean,lat_p50,lat_p90,lat_p95
1776460651.040,64677.4,662,646774,2854.8,3033.0,3943.0,4110.0
1776460661.040,100142.5,1025,1001425,2028.0,1999.0,2445.0,2577.0
1776460671.040,105906.8,1084,1059068,1989.4,1991.0,2354.0,2459.0
1776460681.040,112452.7,1151,1124527,1986.0,1992.0,2339.0,2441.0
1776460691.040,112648.1,1153,1126481,1998.1,2007.0,2360.0,2466.0
1776460701.040,105711.4,1082,1057114,1995.8,2000.0,2350.0,2453.0
1776460711.040,111964.2,1146,1119642,1969.4,1982.0,2315.0,2413.0
1776460721.040,112843.5,1155,1128435,1966.0,1971.0,2326.0,2445.0
1776460731.040,105516.0,1080,1055160,1976.5,1980.0,2345.0,2445.0
1776460741.040,111964.2,1146,1119642,1993.6,2010.0,2358.0,2460.0
1776460751.040,112648.1,1153,1126481,1989.7,1996.0,2348.0,2453.0
1776460761.040,105906.8,1084,1059068,1995.1,1997.0,2347.0,2467.0
1776460771.040,112452.7,1151,1124527,2009.3,2011.0,2363.0,2480.0
1776460781.040,105516.0,1080,1055160,1974.8,1978.0,2325.0,2429.0
1776460791.040,112355.0,1150,1123550,1980.8,1989.0,2334.0,2434.0
1776460801.040,112257.3,1149,1122573,1995.6,2006.0,2350.0,2436.0
1776460811.040,105711.4,1082,1057114,1989.7,1994.0,2347.0,2449.0
1776460821.040,112745.8,1154,1127458,2001.8,2005.0,2357.0,2459.0
1776460831.040,111964.2,1146,1119642,2003.5,2012.0,2352.0,2468.0
1776460841.040,105906.8,1084,1059068,2013.7,2020.0,2366.0,2480.0
1776460851.040,112550.4,1152,1125504,1979.1,1982.0,2326.0,2426.0
1776460861.040,112452.7,1151,1124527,1990.7,1996.0,2344.0,2442.0
1776460871.040,105125.2,1076,1051252,1997.8,2004.0,2354.0,2457.0
1776460881.040,72308.7,763,723087,2116.8,2012.0,2467.0,3300.0
1776460891.040,62326.9,648,623269,2909.7,1999.0,7267.0,9608.0
1776460901.040,54104.9,557,541049,3466.5,2015.0,12591.0,14594.0
1776460911.040,72677.0,744,726770,4111.7,2082.0,17659.0,20049.0
1776460921.040,77378.4,792,773784,5014.2,2106.0,23409.0,25822.0
1776460931.040,85194.4,872,851944,5571.0,2121.0,28979.0,31042.0
1776460941.040,100240.2,1026,1002402,6051.1,2039.0,34287.0,35915.0
1776460951.040,106493.0,1090,1064930,6434.9,2077.0,38111.0,39958.0
1776460961.040,101021.8,1034,1010218,6767.6,2082.0,42474.0,44529.0
1776460971.040,112648.1,1153,1126481,7283.8,2076.0,46479.0,48293.0
1776460981.040,119194.0,1220,1191940,8258.5,2040.0,50060.0,51265.0
1776460991.040,115872.2,1186,1158722,8972.4,2037.0,52927.0,53530.0
1776461001.040,118217.0,1210,1182170,7393.3,2023.0,2583.0,56040.0
1776461011.040,124469.8,1274,1244698,8805.0,2038.0,57355.0,58083.0
1776461021.040,120073.3,1229,1200733,8553.4,2071.0,57792.0,58237.0
1776461031.040,130722.6,1338,1307226,7994.2,2083.0,57106.0,57847.0
1776461041.040,132872.0,1360,1328720,8153.3,2069.0,54919.0,56058.0
1776461051.040,124176.7,1271,1241767,8232.9,2078.0,53007.0,54219.0
1776461061.040,135021.4,1382,1350214,7563.0,2102.0,48966.0,50641.0
1776461071.040,129354.8,1324,1293548,6483.6,2086.0,2657.0,46380.0
1776461081.040,139124.8,1424,1391248,6928.9,2060.0,38446.0,41287.0
1776461091.040,135705.3,1389,1357053,4868.8,2076.0,2558.0,31962.0
1776461101.040,134630.6,1378,1346306,4568.5,2119.0,18457.0,24398.0
1776461111.040,136975.4,1402,1369754,2703.4,2085.0,2524.0,9369.0
1776461121.040,112941.2,1156,1129412,2052.2,2061.0,2399.0,2488.0
1776461131.040,105516.0,1080,1055160,2046.6,2049.0,2400.0,2496.0
1776461141.040,112159.6,1148,1121596,2040.1,2039.0,2380.0,2467.0
1776461151.040,112355.0,1150,1123550,2001.2,2010.0,2321.0,2436.0
1776461161.040,106590.7,1091,1065907,1991.5,1991.0,2308.0,2404.0
1776461171.040,110596.4,1132,1105964,2004.1,2009.0,2336.0,2436.0
1776461181.040,114113.6,1168,1141136,1994.6,2002.0,2311.0,2421.0
1776461191.040,105516.0,1080,1055160,1997.5,1996.0,2315.0,2434.0
1776461201.040,111084.9,1137,1110849,1994.1,1991.0,2326.0,2408.0
1776461211.040,113793.6,1165,1137936,1996.1,1997.0,2321.0,2421.0
1776461221.040,99849.4,1022,998494,2090.5,2067.0,2577.0,2738.0
1776461231.040,91251.8,934,912518,2163.2,2166.0,2682.0,2782.0
1776461241.040,66660.8,684,666608,2400.3,2305.0,2858.0,3880.0
1776461251.040,60394.4,621,603944,3491.9,2413.0,8293.0,9786.0
1776461261.040,50363.2,518,503632,4375.2,2432.0,13660.0,14812.0
1776461271.040,110003.0,1174,1100030,8362.5,2265.0,21372.0,32462.0
1776461281.040,80602.5,825,806025,6427.9,2187.0,23190.0,24842.0
1776461291.040,95941.4,982,959414,6664.3,2141.0,27800.0,29915.0
1776461301.040,92521.9,947,925219,7491.2,2149.0,32924.0,34774.0
1776461311.040,109814.8,1124,1098148,7898.3,2243.0,37476.0,40555.0
1776461321.040,109033.2,1116,1090332,7598.8,2158.0,41490.0,43848.0
1776461331.040,110303.3,1129,1103033,8423.8,2160.0,46159.0,48420.0
1776461341.040,121636.5,1245,1216365,9049.2,2202.0,49474.0,50484.0
1776461351.040,120952.6,1238,1209526,8856.5,2111.0,51986.0,52992.0
1776461361.040,117728.5,1205,1177285,9705.6,2146.0,54603.0,54904.0
1776461371.040,126716.9,1297,1267169,8934.9,2203.0,23766.0,55511.0
1776461381.040,118412.4,1212,1184124,8816.9,2179.0,23226.0,55275.0
1776461391.040,127889.3,1309,1278893,8060.6,2087.0,23463.0,54113.0
1776461401.040,132774.3,1359,1327743,9651.4,2128.0,51222.0,52669.0
1776461411.040,116165.3,1189,1161653,8522.4,2195.0,47449.0,49075.0
1776461421.040,116263.0,1190,1162630,8065.4,2215.0,43720.0,45917.0
1776461431.040,143032.8,1464,1430328,7834.9,2208.0,37733.0,40280.0
1776461441.040,137073.1,1403,1370731,7038.1,2221.0,27693.0,34134.0
1776461451.040,122222.7,1251,1222227,6319.7,2334.0,23118.0,23654.0
1776461461.040,114602.1,1173,1146021,5242.1,2479.0,15428.0,18225.0
1776461471.040,90162.0,923,901620,2713.8,2338.0,3834.0,6136.0
1776461481.040,176250.8,1804,1762508,6319.1,2410.0,21457.0,27205.0
1776461491.040,112843.5,1155,1128435,2241.9,2130.0,2539.0,3817.0
1776461501.040,106590.7,1091,1065907,2247.8,2145.0,2581.0,3581.0
1776461511.040,114309.0,1170,1143090,2207.6,2139.0,2587.0,3245.0
1776461521.040,113625.1,1163,1136251,2156.6,2147.0,2522.0,2653.0
1776461531.040,106199.9,1087,1061999,2135.2,2137.0,2473.0,2564.0
1776461541.040,113136.6,1158,1131366,2127.4,2134.0,2455.0,2529.0
1776461551.040,112745.8,1154,1127458,2097.7,2102.0,2411.0,2491.0
1776461561.040,106395.3,1089,1063953,2066.9,2071.0,2412.0,2475.0
1776461571.040,112355.0,1150,1123550,2357.1,2080.0,3526.0,4719.0
1776461581.040,111378.0,1140,1113780,2032.7,2039.0,2337.0,2433.0
1776461591.040,105418.3,1079,1054183,2045.5,2050.0,2363.0,2448.0
1776461601.040,113136.6,1158,1131366,2076.4,2084.0,2387.0,2483.0
1776461611.040,111671.1,1143,1116711,2063.4,2074.0,2396.0,2495.0
1776461621.040,105883.1,1084,1058831,2058.8,2062.0,2364.0,2483.0
1776461631.040,113136.6,1158,1131366,2057.5,2069.0,2372.0,2477.0
1776461641.040,111573.4,1142,1115734,2070.6,2080.0,2408.0,2500.0
1776461651.040,105906.8,1084,1059068,2082.6,2086.0,2421.0,2485.0
1776461661.040,112648.1,1153,1126481,2083.1,2092.0,2425.0,2495.0
1776461671.040,105613.7,1081,1056137,2103.7,2113.0,2450.0,2515.0
1776461681.040,111671.1,1143,1116711,2092.1,2102.0,2425.0,2505.0
1776461691.040,112648.1,1153,1126481,2096.3,2107.0,2429.0,2493.0
1776461701.040,106786.1,1093,1067861,2100.8,2105.0,2419.0,2497.0
1776461711.040,111182.6,1138,1111826,2034.6,2040.0,2341.0,2435.0
1776461721.040,113429.7,1161,1134297,2040.0,2046.0,2382.0,2495.0
1776461731.040,105906.8,1084,1059068,2038.3,2039.0,2341.0,2440.0
1776461741.040,111378.0,1140,1113780,2198.1,2134.0,2716.0,2873.0
1776461751.040,112452.7,1151,1124527,2068.8,2084.0,2397.0,2489.0
1776461761.040,105711.4,1082,1057114,2064.0,2066.0,2404.0,2501.0
1776461771.040,111964.2,1146,1119642,2087.0,2091.0,2419.0,2513.0
1776461781.040,113136.6,1158,1131366,2082.0,2084.0,2421.0,2481.0
1776461791.040,105906.8,1084,1059068,2072.2,2075.0,2397.0,2489.0
1776461801.040,111475.7,1141,1114757,2065.7,2067.0,2407.0,2501.0
1776461811.040,105906.8,1084,1059068,2050.9,2065.0,2413.0,2469.0
1776461821.040,113136.6,1158,1131366,2073.9,2084.0,2391.0,2481.0
1776461831.040,111768.8,1144,1117688,2085.8,2095.0,2404.0,2493.0
1776461841.040,105809.1,1083,1058091,2078.9,2074.0,2413.0,2472.0
1776461851.040,112452.7,1151,1124527,2083.8,2085.0,2434.0,2505.0
1776461861.040,112257.3,1149,1122573,2094.8,2103.0,2430.0,2502.0
1776461871.040,105613.7,1081,1056137,2102.9,2112.0,2448.0,2535.0
1776461881.040,113332.0,1160,1133320,2069.3,2081.0,2397.0,2481.0
1776461891.040,111573.4,1142,1115734,2041.8,2046.0,2343.0,2436.0
1776461901.040,105222.9,1077,1052229,2030.6,2037.0,2318.0,2430.0
1776461911.040,113234.3,1159,1132343,2067.8,2075.0,2380.0,2472.0
1776461921.040,111280.3,1139,1112803,2043.9,2046.0,2388.0,2484.0
1776461931.040,106590.7,1091,1065907,2040.3,2051.0,2354.0,2422.0
1776461941.040,113234.3,1159,1132343,2043.3,2045.0,2365.0,2459.0
1776461951.040,105320.6,1078,1053206,2043.1,2043.0,2344.0,2429.0
1776461961.040,112061.9,1147,1120619,2054.0,2060.0,2371.0,2465.0
1776461971.040,112648.1,1153,1126481,2103.1,2092.0,2462.0,2578.0
1776461981.040,105711.4,1082,1057114,2081.6,2091.0,2424.0,2498.0
1776461991.040,112257.3,1149,1122573,2086.6,2091.0,2433.0,2515.0
1776462001.040,96136.8,984,961368,2066.1,2067.0,2384.0,2484.0
1776462011.040,76206.0,780,762060,1973.0,1973.0,2258.0,2311.0
1776462021.040,81481.8,834,814818,1969.5,1973.0,2247.0,2288.0
1776462031.040,81481.8,834,814818,1992.0,1992.0,2276.0,2313.0
1776462041.040,13873.4,142,138734,1970.7,1982.0,2216.0,2255.0


"""


# Paste migration events CSV here (format: timestamp_s,round,n_migrations).
CSV_BLOCK_MIGRATION = """
timestamp_s,round,n_migrations
1776460656.236,60,0
1776460674.319,120,0
1776460692.374,180,0
1776460710.376,240,0
1776460728.413,300,0
1776460746.433,360,0
1776460764.438,420,0
1776460782.443,480,0
1776460800.490,540,0
1776460818.510,600,0
1776460836.541,660,0
1776460854.581,720,0
1776460872.591,780,0
1776460890.909,840,8217
1776460908.950,900,41261
1776460926.797,960,35583
1776460944.717,1020,33530
1776460962.670,1080,17835
1776460980.673,1140,13067
1776460998.720,1200,14064
1776461016.711,1260,6709
1776461034.684,1320,9932
1776461052.695,1380,9548
1776461070.742,1440,7743
1776461088.766,1500,7339
1776461106.775,1560,6755
1776461124.742,1620,0
1776461142.757,1680,0
1776461160.782,1740,0
1776461178.798,1800,0
1776461196.799,1860,0
1776461214.843,1920,0
1776461232.835,1980,0
1776461251.250,2040,10813
1776461269.321,2100,40509
1776461287.298,2160,47561
1776461305.269,2220,29925
1776461323.201,2280,19422
1776461341.198,2340,13245
1776461359.215,2400,7303
1776461377.214,2460,8479
1776461395.215,2520,8678
1776461413.198,2580,8584
1776461431.252,2640,7187
1776461449.264,2700,4141
1776461467.197,2760,2191
1776461485.084,2820,0
1776461503.084,2880,0
1776461521.126,2940,0
1776461539.140,3000,0
1776461557.147,3060,0
1776461575.195,3120,0
1776461593.146,3180,0
1776461611.175,3240,0
1776461629.202,3300,0
1776461647.258,3360,0
1776461665.256,3420,0
1776461683.288,3480,0
1776461701.312,3540,0
1776461719.316,3600,0
1776461737.340,3660,0
1776461755.354,3720,0
1776461773.377,3780,0
1776461791.416,3840,0
1776461809.438,3900,0
1776461827.454,3960,0
1776461845.491,4020,0
1776461863.556,4080,0
1776461881.581,4140,0
1776461899.570,4200,0
1776461917.599,4260,0
1776461935.619,4320,0
1776461953.654,4380,0
1776461971.682,4440,0
1776461989.719,4500,0
1776462007.765,4560,0
1776462025.793,4620,0
"""


# Paste the raw tps_timeline.csv content from the baseline (no-LB) run here.
# Format: timestamp_s, tps, n_batches, n_txs, lat_mean, lat_p50, lat_p90, lat_p95
CSV_BLOCK_BASELINE = """

"""

INCLUDED = []

# Dir mode: list subdir labels (without _run_1 suffix) to include in the combined plot.
# Each label must match a subdir in both lb_dir and baseline_dir (e.g. "n4_v_rate_imb60_r110000").
# INCLUDED = [
#     "n4_v_rate_imb90_r110000",
#     "n4_v_rate_imb90_r80000",
#     "n4_v_rate_imb90_r50000",
#     "n4_v_rate_imb90_r20000",
# ]
# INCLUDED = [
#     "n4_v_rate_imb60_r110000",
#     "n4_v_rate_imb90_r110000",
#     "n4_v_rate_imb99_r110000",
# ]
# INCLUDED = [
#     "n4_v_rate_imb90_r110000",
#     "n4_bw_f_rate_imb90_r110000",
#     # "n4_bw_f1_rate_imb90_r10000",
# ]


VALIDATOR_COLORS = ["tab:blue", "tab:orange", "tab:green", "tab:red",
                    "tab:purple", "tab:brown", "tab:pink", "tab:gray"]

_BALANCED_END_RE = re.compile(r'Balanced phase end spread: \d+ ms \((.+?)\)')
_REGION_OFFSET_RE = re.compile(r'R\d+:([\d.]+)s')


def _parse_balanced_phase_end(log_path):
    """Return the minimum balanced-phase-end offset (seconds after start), or None."""
    text = Path(log_path).read_text()
    m = _BALANCED_END_RE.search(text)
    if not m:
        return None
    offsets = _REGION_OFFSET_RE.findall(m.group(1))
    if not offsets:
        return None
    return min(float(v) for v in offsets)


def parse_raw_csv(text):
    """Parse raw CSV text (no block marker) into a list of dicts with valid timestamp_s."""
    lines = [line for line in text.strip().splitlines() if line.strip()]
    if len(lines) < 2:
        return None
    rows = []
    for row in csv.DictReader(lines):
        timestamp = (row.get("timestamp_s") or "").strip()
        if not timestamp:
            continue
        try:
            float(timestamp)
        except ValueError:
            continue
        rows.append(row)
    return rows or None


def _draw_imbalance_markers(axes):
    for ax in axes:
        for marker_s in IMBALANCE_MARKERS_S:
            ax.axvline(marker_s, color="black", linestyle="--", linewidth=0.8, alpha=0.7)


def plot_timeline(tps_rows, mig_rows, label, output, latency_col, baseline_rows=None, warmup_s=None):
    tps_rows = sorted(tps_rows, key=lambda row: float(row["timestamp_s"]))
    t0 = float(tps_rows[0]["timestamp_s"])

    assert "tps" in tps_rows[0], "CSV_BLOCK must have a 'tps' column"

    tps_rows = [row for row in tps_rows if float(row["tps"]) <= TPS_OUTLIER]
    assert tps_rows, f"All LB TPS rows filtered (threshold={TPS_OUTLIER})"
    tps_times = [float(row["timestamp_s"]) - t0 for row in tps_rows]

    # Pre-process baseline so baseline_times is available for the latency section
    baseline_times = []
    if baseline_rows:
        baseline_rows = sorted(baseline_rows, key=lambda row: float(row["timestamp_s"]))
        baseline_rows = [row for row in baseline_rows if float(row["tps"]) <= TPS_OUTLIER]
        assert baseline_rows, f"All baseline TPS rows filtered (threshold={TPS_OUTLIER})"
        baseline_t0 = float(baseline_rows[0]["timestamp_s"])
        baseline_times = [float(row["timestamp_s"]) - baseline_t0 for row in baseline_rows]

    # Determine latency data before building the figure so we know how many panels to create
    lat_data = []       # (times, values, label, linestyle, marker) for each series
    if latency_col:
        col = f"lat_{latency_col}"
        lat_pairs = [
            (t, float(row[col]))
            for t, row in zip(tps_times, tps_rows)
            if row.get(col, "").strip()
        ]
        if lat_pairs:
            lt, lv = zip(*lat_pairs)
            lat_data.append((list(lt), list(lv), f"Commit latency {latency_col} (ms)", "-", "o"))
        if baseline_rows:
            bl_lat_pairs = [
                (t, float(row[col]))
                for t, row in zip(baseline_times, baseline_rows)
                if row.get(col, "").strip()
            ]
            if bl_lat_pairs:
                blt, blv = zip(*bl_lat_pairs)
                lat_data.append((list(blt), list(blv), f"Baseline commit latency {latency_col} (ms)", "--", "x"))

    n_panels = 2 + (1 if lat_data else 0)
    height_ratios = [3, 2, 2] if lat_data else [3, 2]
    fig, axes = plt.subplots(
        n_panels, 1, figsize=(4.5, 1.2 * n_panels + 0.6), sharex=True,
        gridspec_kw={"height_ratios": height_ratios},
    )
    ax_tps = axes[0]
    ax_lat = axes[1] if lat_data else None
    ax_mig = axes[-1]

    # --- TPS panel ---
    total_tps_vals = [float(row["tps"]) for row in tps_rows]
    pt, pv = _smoothed_series(tps_times, total_tps_vals)
    ax_tps.plot(pt, pv, color=COLORS[0], linestyle="-", linewidth=1.0)
    if baseline_rows:
        baseline_tps_vals = [float(row["tps"]) for row in baseline_rows]
        pt, pv = _smoothed_series(baseline_times, baseline_tps_vals)
        ax_tps.plot(pt, pv, color=COLORS[0], linestyle="--", linewidth=0.8)
    ax_tps.set_ylabel("Throughput [ktrans/s]", fontsize=6)
    ax_tps.yaxis.set_major_formatter(ticker.FuncFormatter(lambda x, _: f"{x/1000:.0f}"))
    ax_tps.grid(True)

    # --- Latency panel ---
    if ax_lat is not None:
        for lt, lv, lbl, ls, _mk in lat_data:
            pt, pv = _smoothed_series(lt, lv)
            ax_lat.plot(pt, pv, color=COLORS[1], linestyle=ls, linewidth=1.0 if ls == "-" else 0.8)
        ax_lat.set_ylabel(f"Latency {latency_col} [s]", fontsize=6)
        ax_lat.yaxis.set_major_formatter(ticker.FuncFormatter(lambda x, _: f"{x/1000:.1f}"))
        ax_lat.grid(True)

    # --- Migrations panel ---
    if mig_rows:
        mig_rows = sorted(mig_rows, key=lambda row: float(row["timestamp_s"]))
        mig_times = [float(row["timestamp_s"]) - t0 for row in mig_rows]
        mig_counts = [int(row["n_migrations"]) for row in mig_rows]
        cumulative = list(np.cumsum(mig_counts))
        ax_mig.step([0.0] + mig_times, [0] + cumulative,
                    where="post", color=COLORS[2], linewidth=1.0)
    else:
        ax_mig.text(
            0.5, 0.5, "no migrations", transform=ax_mig.transAxes,
            ha="center", va="center", fontsize=7, color="gray",
        )
    ax_mig.set_ylabel("Cumul. Migs", fontsize=6)
    ax_mig.yaxis.set_major_formatter(ticker.FuncFormatter(
        lambda x, _: f"{x/1000:.0f}k" if x >= 1000 else f"{x:.0f}"
    ))
    ax_mig.set_xlabel("Time (s)")
    ax_mig.grid(True)

    for ax in axes:
        ax.yaxis.set_major_locator(MaxNLocator(nbins=3))
        ax.xaxis.set_major_locator(MaxNLocator(nbins=6))

    max_t = tps_times[-1] if tps_times else 0.0
    if baseline_times:
        max_t = max(max_t, baseline_times[-1])
    left  = TRIM_LEFT_DUR
    right = max_t - TRIM_RIGHT_DUR
    if right > left:
        ax_tps.set_xlim(left=left, right=right)

    _draw_imbalance_markers(axes)

    fig.subplots_adjust(left=0.12, right=0.97, top=0.96, bottom=0.12, hspace=0.18)
    plt.savefig(output, dpi=200, bbox_inches="tight", pad_inches=0.02)
    pdf_path = Path(output).with_suffix(".pdf")
    plt.savefig(pdf_path, bbox_inches="tight", pad_inches=0.02)
    print(f"Saved: {pdf_path.resolve()}")


def plot_from_dirs(lb_dir, baseline_dir, title, output, lat_col, warmup_s):
    assert INCLUDED, "INCLUDED is empty — add labels to plot"
    lb_dir = Path(lb_dir)
    baseline_dir = Path(baseline_dir)

    series = []  # list of (label, lb_data, bl_data)
    for label in INCLUDED:
        lb_run_dir   = lb_dir   / f"{label}_run_1"
        base_run_dir = baseline_dir / f"{label}_run_1"
        assert lb_run_dir.exists(),   f"LB run dir not found: {lb_run_dir.resolve()}"
        assert base_run_dir.exists(), f"Baseline run dir not found: {base_run_dir.resolve()}"
        effective_lat = lat_col or "p90"
        lb_data = load_run(lb_run_dir,   effective_lat)
        bl_data = load_run(base_run_dir, effective_lat)
        series.append((label, lb_data, bl_data))

    if warmup_s is None:
        candidates = []
        for label in INCLUDED:
            for run_dir in (lb_dir / f"{label}_run_1", baseline_dir / f"{label}_run_1"):
                v = _parse_balanced_phase_end(run_dir / "output.log")
                if v is not None:
                    candidates.append(v)
        if candidates:
            warmup_s = min(candidates)

    show_lat = lat_col is not None
    n_panels = 3 if show_lat else 2
    height_ratios = [3, 2, 2] if show_lat else [3, 2]
    fig, axes = plt.subplots(
        n_panels, 1, figsize=(4.5, 1.2 * n_panels + 0.6), sharex=True,
        gridspec_kw={"height_ratios": height_ratios},
    )
    ax_tps = axes[0]
    ax_lat = axes[1] if show_lat else None
    ax_mig = axes[-1]

    max_t = 0.0
    any_mig = False
    color_handles = []

    for i, (label, lb_data, bl_data) in enumerate(series):
        color = COLORS[i % len(COLORS)]
        lb_tps_times, lb_tps_vals, lb_lat_pairs, mig_times, mig_cumulative = lb_data
        bl_tps_times, bl_tps_vals, bl_lat_pairs, _, _ = bl_data

        if lb_tps_times:
            max_t = max(max_t, lb_tps_times[-1])
        if bl_tps_times:
            max_t = max(max_t, bl_tps_times[-1])

        pt, pv = _smoothed_series(lb_tps_times, lb_tps_vals)
        ax_tps.plot(pt, pv, color=color, linestyle="-", linewidth=1.0)
        pt, pv = _smoothed_series(bl_tps_times, bl_tps_vals)
        ax_tps.plot(pt, pv, color=color, linestyle="--", linewidth=0.8)

        if show_lat:
            if lb_lat_pairs:
                lt, lv = zip(*lb_lat_pairs)
                pt, pv = _smoothed_series(list(lt), list(lv))
                ax_lat.plot(pt, pv, color=color, linestyle="-", linewidth=1.0)
            if bl_lat_pairs:
                bt, bv = zip(*bl_lat_pairs)
                pt, pv = _smoothed_series(list(bt), list(bv))
                ax_lat.plot(pt, pv, color=color, linestyle="--", linewidth=0.8)

        if mig_times:
            any_mig = True
            ax_mig.step([0.0] + mig_times, [0] + mig_cumulative, where="post",
                        color=color, linewidth=1.0)

        color_handles.append(
            mlines.Line2D([], [], color=color, linewidth=1.0, label=label)
        )

    if not any_mig:
        ax_mig.text(0.5, 0.5, "no migrations", transform=ax_mig.transAxes,
                    ha="center", va="center", fontsize=7, color="gray")

    left  = TRIM_LEFT_DUR
    right = max_t - TRIM_RIGHT_DUR
    if right > left:
        ax_tps.set_xlim(left=left, right=right)

    if title:
        ax_tps.set_title(title)
    ax_tps.set_ylabel("Throughput [ktrans/s]", fontsize=6)
    ax_tps.yaxis.set_major_formatter(ticker.FuncFormatter(lambda x, _: f"{x/1000:.0f}"))
    ax_tps.grid(True)

    if show_lat and ax_lat is not None:
        ax_lat.set_ylabel(f"Latency {lat_col} [s]", fontsize=6)
        ax_lat.yaxis.set_major_formatter(ticker.FuncFormatter(lambda x, _: f"{x/1000:.1f}"))
        ax_lat.grid(True)

    ax_mig.set_ylabel("Cumul. Migs", fontsize=6)
    ax_mig.yaxis.set_major_formatter(ticker.FuncFormatter(
        lambda x, _: f"{x/1000:.0f}k" if x >= 1000 else f"{x:.0f}"
    ))
    ax_mig.set_xlabel("Time (s)")
    ax_mig.grid(True)

    for ax in axes:
        ax.yaxis.set_major_locator(MaxNLocator(nbins=3))
        ax.xaxis.set_major_locator(MaxNLocator(nbins=6))

    _draw_imbalance_markers(axes)

    fig.subplots_adjust(left=0.12, right=0.97, top=0.96, bottom=0.12, hspace=0.18)
    plt.savefig(output, dpi=200, bbox_inches="tight", pad_inches=0.02)
    pdf_path = Path(output).with_suffix(".pdf")
    plt.savefig(pdf_path, bbox_inches="tight", pad_inches=0.02)
    print(f"Saved: {Path(output).resolve()}")
    print(f"Saved: {pdf_path.resolve()}")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "lb_dir",
        nargs="?",
        default=None,
        help="(Dir mode) Results dir from fig_3_tps_timeline_lb.sh.",
    )
    parser.add_argument(
        "baseline_dir",
        nargs="?",
        default=None,
        help="(Dir mode) Results dir from fig_3_tps_timeline_baseline.sh.",
    )
    parser.add_argument(
        "--label",
        default="pasted_log",
        help="Label to show in the plot title.",
    )
    parser.add_argument(
        "-o",
        "--output",
        default=None,
        help="Output PNG path.",
    )
    parser.add_argument(
        "--latency",
        choices=["mean", "p50", "p90", "p95"],
        default="mean",
        help="Latency metric to show in the latency panel (default: mean).",
    )
    parser.add_argument(
        "--baseline",
        action="store_true",
        help="Overlay baseline (no-LB) TPS from CSV_BLOCK_BASELINE on the main LB TPS plot.",
    )
    parser.add_argument(
        "--warmup",
        type=float,
        default=None,
        help="Deprecated; vertical dashed markers are hardcoded at 240s and 600s.",
    )
    args = parser.parse_args()

    output = args.output or f"tps_migration_{args.label}.png"
    _apply_sigmod_style()

    use_dir_mode = not (CSV_BLOCK.strip() or CSV_BLOCK_MIGRATION.strip() or CSV_BLOCK_BASELINE.strip())
    if use_dir_mode:
        assert args.lb_dir,       "Dir mode: provide lb_dir as first positional argument"
        assert args.baseline_dir, "Dir mode: provide baseline_dir as second positional argument"
        plot_from_dirs(args.lb_dir, args.baseline_dir, args.label, output, args.latency, args.warmup)
        return

    assert CSV_BLOCK.strip(), "No log input provided in CSV_BLOCK"
    tps_rows = parse_raw_csv(CSV_BLOCK)
    mig_rows = parse_raw_csv(CSV_BLOCK_MIGRATION) or []
    assert tps_rows, "No valid TPS rows found in CSV_BLOCK"

    baseline_rows = None
    if args.baseline:
        assert CSV_BLOCK_BASELINE.strip(), "CSV_BLOCK_BASELINE is empty — paste baseline tps_timeline.py output into it"
        baseline_rows = parse_raw_csv(CSV_BLOCK_BASELINE)
        assert baseline_rows, "No valid rows found in CSV_BLOCK_BASELINE"

    plot_timeline(tps_rows, mig_rows, args.label, output, args.latency, baseline_rows, args.warmup)
    print(f"Saved to {Path(output).resolve()}")


if __name__ == "__main__":
    main()
