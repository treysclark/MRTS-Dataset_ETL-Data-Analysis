SELECT (SELECT COUNT(*) FROM combined_sales) + (SELECT COUNT(*) FROM sales) + (SELECT COUNT(*) * 12 FROM nan)