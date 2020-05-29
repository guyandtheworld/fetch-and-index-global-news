```
    uuid = models.UUIDField(
        primary_key=True, default=uuid.uuid4, editable=False)
    story_id = models.UUIDField()
    story_title = models.CharField(max_length=2000)
    story_url = models.CharField(max_length=1000)
    published_date = models.DateTimeField()  # timezone localized date
    domain = models.CharField(max_length=150)
    language = models.CharField(max_length=100)
    source_country = models.CharField(max_length=100)
    entity_name = models.CharField(max_length=200)
    entity_id = models.UUIDField()
    scenario_id = models.UUIDField()
    story_body = models.CharField(max_length=10000)
    timestamp = models.DateTimeField()
    cluster = models.IntegerField()
    scores = JSONField()  # gross score, source score
    entities = JSONField()
    hotness = JSONField()
    bucket_scores = JSONField()
    sentiment = JSONField()
```
