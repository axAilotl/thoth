from core.wiki_io import markdown_file_link


def test_source_link_escapes_literal_filename_characters():
    assert markdown_file_link('Paper [draft]', '../vault/Paper (draft) 20%.pdf') == (
        r'[Paper \[draft\]](../vault/Paper%20%28draft%29%2020%25.pdf)'
    )
    assert markdown_file_link('Guide', '../vault/Guide, +example.md') == '[Guide](../vault/Guide,%20+example.md)'
    assert markdown_file_link('Diarization', '../vault/Solutions & APIs.md') == '[Diarization](../vault/Solutions%20&%20APIs.md)'
