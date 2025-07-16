#!/bin/bash

# Bài 1: Sắp xếp các bộ phim theo ngày phát hành giảm dần
awk '{
    in_quotes = 0;
    output = "";
    for (i = 1; i <= length($0); i++) {
        char = substr($0, i, 1);
        if (char == "\"") in_quotes = !in_quotes;
        if (char == "," && in_quotes) char = "##";
        output = output char;
    }
    print output;
}' tmdb-movies.csv | sort -t ',' -k16,16r > bai1.txt

# Bài 2: Lọc ra các bộ phim có đánh giá trung bình trên 7.5
awk '{
    in_quotes = 0;
    output = "";
    for (i = 1; i <= length($0); i++) {
        char = substr($0, i, 1);
        if (char == "\"") in_quotes = !in_quotes;
        if (char == "," && in_quotes) char = "##";
        output = output char;
    }
    print output;
}' tmdb-movies.csv | awk -F ',' '$18 > 7.5' > bai2.txt

# Bài 3: Tìm phim có doanh thu cao nhất và thấp nhất
awk '{
    in_quotes = 0;
    output = "";
    for (i = 1; i <= length($0); i++) {
        char = substr($0, i, 1);
        if (char == "\"") in_quotes = !in_quotes;
        if (char == "," && in_quotes) char = "##";
        output = output char;
    }
    print output;
}' tmdb-movies.csv | tail -n +2 | sort -t ',' -k5,5nr | head -n 1 > bai3.txt

awk '{
    in_quotes = 0;
    output = "";
    for (i = 1; i <= length($0); i++) {
        char = substr($0, i, 1);
        if (char == "\"") in_quotes = !in_quotes;
        if (char == "," && in_quotes) char = "##";
        output = output char;
    }
    print output;
}' tmdb-movies.csv | tail -n +2 | sort -t ',' -k5,5nr | tail -n 1 >> bai3.txt

# Bài 4: Tính tổng doanh thu tất cả các bộ phim
awk '{
    in_quotes = 0;
    output = "";
    for (i = 1; i <= length($0); i++) {
        char = substr($0, i, 1);
        if (char == "\"") in_quotes = !in_quotes;
        if (char == "," && in_quotes) char = "##";
        output = output char;
    }
    print output;
}' tmdb-movies.csv | tail -n +2 | awk -F ',' '$5 ~ /^[0-9]+(\.[0-9]+)?$/ { sum += $5 } END { printf "%.0f\n", sum }' > bai4.txt

# Bài 5: Top 10 bộ phim đem về lợi nhuận cao nhất
awk '{
    in_quotes = 0;
    output = "";
    for (i = 1; i <= length($0); i++) {
        char = substr($0, i, 1);
        if (char == "\"") in_quotes = !in_quotes;
        if (char == "," && in_quotes) char = "##";
        output = output char;
    }
    print output;
}' tmdb-movies.csv | tail -n +2 | awk -F ',' '{profit = $5 - $4; print $0 "," profit}' | sort -t ',' -k22,22nr | head -n 10 | sed 's/,[^,]*$//' > bai5.txt

# Bài 6: Tìm đạo diễn có nhiều phim nhất và diễn viên đóng nhiều phim nhất
awk '{
    in_quotes = 0;
    output = "";
    for (i = 1; i <= length($0); i++) {
        char = substr($0, i, 1);
        if (char == "\"") in_quotes = !in_quotes;
        if (char == "," && in_quotes) char = "##";
        output = output char;
    }
    print output;
}' tmdb-movies.csv | tail -n +2 | cut -d ',' -f9 | awk -F ',' '{count[$1]++} END {for (val in count) print val "#" count[val]}' > tmdb-director.csv

sort -t '#' -k2,2nr tmdb-director.csv | head -n 1 > bai6a.txt

awk '{
    in_quotes = 0;
    output = "";
    for (i = 1; i <= length($0); i++) {
        char = substr($0, i, 1);
        if (char == "\"") in_quotes = !in_quotes;
        if (char == "," && in_quotes) char = "##";
        output = output char;
    }
    print output;
}' tmdb-movies.csv | tail -n +2 | cut -d ',' -f7 > tmdb-cast.csv

awk -F '|' '{for (i=1; i<=NF; i++) print $i}' tmdb-cast.csv | sort | uniq -c | sort -k1,1nr | head -n 1 > bai6b.txt

# Bài 7: Thống kê số lượng phim theo thể loại
awk '{
    in_quotes = 0;
    output = "";
    for (i = 1; i <= length($0); i++) {
        char = substr($0, i, 1);
        if (char == "\"") in_quotes = !in_quotes;
        if (char == "," && in_quotes) char = "##";
        output = output char;
    }
    print output;
}' tmdb-movies.csv | tail -n +2 | cut -d ',' -f14 > tmdb-genres.csv

awk -F '|' '{for (i=1; i<=NF; i++) print $i}' tmdb-genres.csv | sort | uniq -c | sort -k1,1nr > bai7.txt
