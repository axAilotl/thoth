from core.chunking import chunk_text


def test_chunk_text_produces_stable_ids_for_same_input():
    text = "alpha\nbeta\ngamma\ndelta"

    first = chunk_text(text, chunk_size=12, namespace="transcript")
    second = chunk_text(text, chunk_size=12, namespace="transcript")

    assert [chunk.text for chunk in first] == [chunk.text for chunk in second]
    assert [chunk.chunk_id for chunk in first] == [chunk.chunk_id for chunk in second]
    assert [chunk.index for chunk in first] == [1, 2]
    assert all(chunk.chunk_id.startswith("transcript_") for chunk in first)


def test_chunk_text_caps_long_lines_without_losing_order():
    text = "a" * 25

    chunks = chunk_text(text, chunk_size=10, namespace="pdf")

    assert [chunk.text for chunk in chunks] == ["a" * 10, "a" * 10, "a" * 5]
    assert [chunk.index for chunk in chunks] == [1, 2, 3]
    assert all(len(chunk.text) <= 10 for chunk in chunks)
    assert "".join(chunk.text for chunk in chunks) == text
