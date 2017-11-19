# SentinelDL
### A Python download client for ESA Sentinel scihub data
-
<b>USAGE: SentinelDL.py [-h|-H|--help] URI  </b>  
 URI - a scihub URL for searching products  
 <i>or</i>  
 a scihub URL for downloading product  
 <i>or</i>  
 a file containing metalinks of products.  
  
<b>INSTRUCTIONS:</b>  

1. Go to https://scihub.copernicus.eu/ and register.
2. create a file .credentials with a single line containing
   \[USER\]:\[PASSWORD\]
   where USER and PASSWORD are the scihub credentials.
3. Enter the hub at https://scihub.copernicus.eu/dhus/ and log in.
4.  Search for data.
1.  Add data to your cart.
1.  Save cart to a file (by default products.meta4).
1.  Run the code with the meta4 file name as a parameter.
1.  Instead of steps 5-7, provide product link as a parameter.
1.  Optionally, use the search string as URI

More data and also how to build a search URL are available at:
https://scihub.copernicus.eu/userguide/
*and*
https://scihub.copernicus.eu/userguide/BatchScripting
  
*By Ran Novitsky Nof (ran.nof@gmail.com) @ BSL, 2015*
