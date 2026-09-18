import math

import pytest

from src.common.metrics import (
    coverage,
    dcg_at_k,
    ndcg_at_k,
    precision_at_k,
    recall_at_k,
)


def test_precision_counts_hits_over_k():
    # gợi ý [1,2,3,4], liên quan {1,3} -> 2 trúng / 4 = 0.5
    assert precision_at_k([1, 2, 3, 4], {1, 3}, k=4) == 0.5


def test_precision_divides_by_k_not_list_length():
    # Chỉ gợi ý được 1 phim nhưng k=10: precision là 1/10, không phải 1/1.
    # Chia cho len(list) sẽ thưởng cho mô hình gợi ý ít — đó là cài sai.
    assert precision_at_k([1], {1}, k=10) == pytest.approx(0.1)


def test_recall_divides_by_number_of_relevant():
    # liên quan {1,3,5}, gợi ý bắt được 1 và 3 -> 2/3
    assert recall_at_k([1, 2, 3, 4], {1, 3, 5}, k=4) == pytest.approx(2 / 3)


def test_dcg_discounts_by_position():
    # vị trí 0 -> 1/log2(2) = 1.0 ; vị trí 2 -> 1/log2(4) = 0.5
    assert dcg_at_k([1, 2, 3], {1, 3}, k=3) == pytest.approx(1.5)


def test_ndcg_hand_computed():
    # gợi ý [1,2,3], liên quan {1,3}
    # DCG  = 1/log2(2) + 0 + 1/log2(4) = 1.0 + 0.5      = 1.5
    # IDCG = 1/log2(2) + 1/log2(3)     = 1.0 + 0.6309298 = 1.6309298
    # NDCG = 1.5 / 1.6309298 = 0.9197208
    assert ndcg_at_k([1, 2, 3], {1, 3}, k=3) == pytest.approx(0.9197208, abs=1e-6)


def test_ndcg_is_one_when_relevant_items_are_ranked_first():
    assert ndcg_at_k([1, 3, 2], {1, 3}, k=3) == pytest.approx(1.0)


def test_ndcg_idcg_is_capped_at_k():
    # 5 phim liên quan nhưng k=2: IDCG chỉ tính 2 vị trí, nên gợi ý 2 phim
    # liên quan đầu bảng vẫn phải cho NDCG = 1.0
    assert ndcg_at_k([1, 2], {1, 2, 3, 4, 5}, k=2) == pytest.approx(1.0)


def test_metrics_are_zero_when_nothing_is_relevant():
    assert precision_at_k([1, 2], set(), k=2) == 0.0
    assert recall_at_k([1, 2], set(), k=2) == 0.0
    assert ndcg_at_k([1, 2], set(), k=2) == 0.0


def test_metrics_handle_empty_recommendations():
    assert precision_at_k([], {1}, k=10) == 0.0
    assert recall_at_k([], {1}, k=10) == 0.0
    assert ndcg_at_k([], {1}, k=10) == 0.0


def test_coverage_counts_distinct_items_over_catalog():
    assert coverage([1, 1, 2, 3], catalog_size=10) == pytest.approx(0.3)


def test_coverage_of_empty_catalog_is_zero():
    assert coverage([1, 2], catalog_size=0) == 0.0


def test_coverage_can_exceed_one_when_catalog_size_is_wrong():
    # coverage KHÔNG bị kẹp ở 1.0: nếu catalog_size truyền vào nhỏ hơn thực tế
    # (lỗi của bên gọi), kết quả > 1.0 là tín hiệu báo lỗi, không phải giá trị
    # cần diễn giải như một metric hợp lệ. Kẹp về 1.0 sẽ che giấu lỗi này.
    assert coverage([1, 2, 3], catalog_size=2) == pytest.approx(1.5)


def test_hits_count_distinct_items_not_occurrences():
    # Danh sách gợi ý lỡ chứa phim trùng lặp: đếm theo lần xuất hiện sẽ cho
    # recall_at_k([1,1],{1},k=2) = 2/1 = 2.0, vượt khoảng [0,1] hợp lệ.
    # ALS top-k không trùng lặp trong thực tế, nhưng metric này không được
    # phép trả về giá trị vô nghĩa dù đầu vào có sai.
    assert recall_at_k([1, 1], {1}, k=2) == pytest.approx(1.0)
    assert precision_at_k([1, 1], {1}, k=2) == pytest.approx(0.5)


def test_metrics_handle_negative_k():
    assert precision_at_k([1, 2], {1}, k=-1) == 0.0
    assert recall_at_k([1, 2], {1}, k=-1) == 0.0
    assert ndcg_at_k([1, 2], {1}, k=-1) == 0.0
