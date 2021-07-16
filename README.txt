# Script to compute Population and POI data on grid and in 5km2 area

2021-07-15  Created by Tawan.T
2021-07-16  Add function to compute #shops by specified radius
2021-07-16  Add reverse geocoding by  longdo map

Note:
1.Result of total shop computed by specified radius function might be different from that by summation on grids as the first method uses location lat lng at center as reference but the second uses fixed grid location as reference.
2.Restaurant considered in the category: alcohols in ("liquer","beer","wine")  

WHY:
- With input as Lat Lng of Store branches, this script can get data useful in analyzing sales area or finding new store locations.

How:
1.python Search_Population_POI_by_LatLng_H3Grid.py
