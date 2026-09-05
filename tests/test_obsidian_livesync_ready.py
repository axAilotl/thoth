from scripts.obsidian_livesync_ready import ready_from_output


def test_requires_ready_and_configured_timer():
    assert ready_from_output('=> {"ready":true,"periodic":true,"timer":true}\n')
    assert ready_from_output('=> {"ready":true,"periodic":false,"timer":false}\n')
    assert not ready_from_output('=> {"ready":true,"periodic":true,"timer":false}\n')
    assert not ready_from_output('=> {"ready":false}\n')
    assert not ready_from_output('(no output)')
