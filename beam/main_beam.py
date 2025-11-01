import yaml
import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions

# Load config
with open('pipeline.yaml') as f:
    config = yaml.safe_load(f)

options = PipelineOptions(
    runner=config['pipeline']['runner'],
    project=config['pipeline']['project'],
    temp_location=config['pipeline']['temp_location']
)

with beam.Pipeline(options=options) as p:
    lines = p | "ReadFromHDFS" >> beam.io.ReadFromText(config['input']['source'][0]['path'])

    words = (
        lines
        | "Tokenize" >> beam.FlatMap(lambda line: [w.strip('.,?!-:;()[]{}"\'').lower() for w in line.split() if w])
    )

    word_counts = (
        words
        | "PairWithOne" >> beam.Map(lambda w: (w, 1))
        | "CountWords" >> beam.CombinePerKey(sum)
        | "FormatOutput" >> beam.Map(lambda kv: f"{kv[0]}: {kv[1]}")
    )

    word_counts | "WriteToHDFS" >> beam.io.WriteToText(config['output']['destination']['path'])
