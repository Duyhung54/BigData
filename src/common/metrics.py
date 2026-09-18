"""Metrics xếp hạng cho hệ gợi ý.

Python thuần, cố ý KHÔNG phụ thuộc pyspark: đây là phần logic dễ cài sai nhất
mà vẫn cho ra con số trông hợp lý, nên nó phải test được trong một giây.

Quy ước độ liên quan là nhị phân: một phim là "liên quan" nếu user chấm
>= config.RELEVANCE_THRESHOLD (4.0 sao). Việc chọn ngưỡng nằm ở phía gọi hàm.
"""
import math
from typing import Iterable, Sequence, Set


def _hits(recommended: Sequence, relevant: Set, k: int) -> int:
    """Đếm số phim liên quan PHÂN BIỆT nằm trong top-k.

    Đếm phân biệt chứ không đếm số lần xuất hiện: nếu danh sách gợi ý lỡ chứa
    một phim hai lần, cách đếm theo lần xuất hiện sẽ cho recall_at_k([1,1],{1},k=2)
    = 2/1 = 2.0, tức vượt khoảng [0,1] hợp lệ mà không có gì báo lỗi. Danh sách
    top-k của ALS không trùng lặp, nhưng metrics này sinh số cho báo cáo nên
    không được phép trả về giá trị vô nghĩa dù đầu vào có sai.
    """
    return len({item for item in list(recommended)[:k] if item in relevant})


def precision_at_k(recommended: Sequence, relevant: Set, k: int) -> float:
    """Tỷ lệ phim liên quan trong top-k.

    Mẫu số là k, KHÔNG phải len(recommended): chia cho độ dài danh sách sẽ
    thưởng cho mô hình gợi ý ít phim.
    """
    if k <= 0 or not relevant:
        return 0.0
    return _hits(recommended, relevant, k) / k


def recall_at_k(recommended: Sequence, relevant: Set, k: int) -> float:
    """Tỷ lệ phim liên quan của user được bắt trúng trong top-k."""
    if k <= 0 or not relevant:
        return 0.0
    return _hits(recommended, relevant, k) / len(relevant)


def dcg_at_k(recommended: Sequence, relevant: Set, k: int) -> float:
    """Discounted Cumulative Gain với độ liên quan nhị phân."""
    if k <= 0:
        return 0.0
    return sum(
        1.0 / math.log2(position + 2)
        for position, item in enumerate(list(recommended)[:k])
        if item in relevant
    )


def ndcg_at_k(recommended: Sequence, relevant: Set, k: int) -> float:
    """DCG chuẩn hoá theo thứ tự lý tưởng.

    IDCG bị chặn ở min(k, số phim liên quan): nếu user có 50 phim liên quan
    mà k=10, danh sách hoàn hảo chỉ có thể chứa 10 phim, nên IDCG phải tính
    trên 10 vị trí. Không chặn ở k là lỗi phổ biến làm NDCG luôn nhỏ hơn 1.
    """
    if k <= 0 or not relevant:
        return 0.0
    ideal_positions = min(k, len(relevant))
    idcg = sum(1.0 / math.log2(position + 2) for position in range(ideal_positions))
    if idcg == 0.0:
        return 0.0
    return dcg_at_k(recommended, relevant, k) / idcg


def coverage(recommended_items: Iterable, catalog_size: int) -> float:
    """Tỷ lệ catalog mà hệ thống thực sự gợi ý tới.

    Phát hiện mô hình chỉ quanh quẩn vài phim nổi tiếng: RMSE có thể rất đẹp
    trong khi coverage chỉ 2%, nghĩa là mọi user đều nhận cùng một danh sách.

    Giả định đầu vào: mọi phim trong recommended_items được kỳ vọng nằm trong
    catalog đã đếm bằng catalog_size. Kết quả > 1.0 nghĩa là bên gọi truyền
    catalog_size sai (nhỏ hơn thực tế) — đây là lỗi của bên gọi cần sửa, KHÔNG
    phải một giá trị metric cần diễn giải. Hàm này cố ý không kẹp về 1.0, vì
    kẹp sẽ che giấu lỗi thay vì phơi bày nó.
    """
    if catalog_size <= 0:
        return 0.0
    return len(set(recommended_items)) / catalog_size
