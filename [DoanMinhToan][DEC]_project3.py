import pandas as pd

df = pd.read_csv("tmdb-movies.csv")

df["release_date"] = pd.to_datetime(df["release_date"], format="%Y-%m-%d", errors="coerce")
df_sorted = df.sort_values(by="release_date", ascending=False)
df_sorted.to_csv("tmdb-movies-sorted.csv", index=False)

df_high_rated = df[df["vote_average"] > 7.5]
df_high_rated.to_csv("tmdb-high-rated.csv", index=False)

highest_revenue_movie = df.loc[df["revenue"].idxmax()]
lowest_revenue_movie = df.loc[df["revenue"].idxmin()]

total_revenue = df["revenue"].sum()

df["profit"] = df["revenue"] - df["budget"]
top_10_profitable_movies = df.sort_values(by="profit", ascending=False).head(10)

top_director = df["director"].value_counts().idxmax()
top_actor = df["cast"].str.split("|").explode().value_counts().idxmax()

genre_counts = df["genres"].str.split("|").explode().value_counts()

print(f"Phim có doanh thu cao nhất: {highest_revenue_movie['original_title']}, doanh thu: {highest_revenue_movie['revenue']}")
print(f"Phim có doanh thu thấp nhất: {lowest_revenue_movie['original_title']}, doanh thu: {lowest_revenue_movie['revenue']}")
print(f"Tổng doanh thu của tất cả các bộ phim: {total_revenue}")
print("Top 10 bộ phim đem về lợi nhuận cao nhất:")
print(top_10_profitable_movies[["original_title", "profit"]])

print(f"Đạo diễn có nhiều bộ phim nhất: {top_director}")
print(f"Diễn viên đóng nhiều phim nhất: {top_actor}")

print("Số lượng phim theo thể loại:")
print(genre_counts)
