from deploy import get_api_key, graphql

key = get_api_key()
for pid in ['5uyxrbeynikj5m', 'ec20l54rwsxcfv']:
    q = """mutation ResumePod($input: PodResumeInput!) { podResume(input: $input) { id desiredStatus } }"""
    r = graphql(key, q, {'input': {'podId': pid, 'gpuCount': 1}})
    if 'errors' in r:
        print(f'{pid}: {r["errors"]}')
    else:
        print(f'{pid}: RESUMING')
