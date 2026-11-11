from markdown_pdf import MarkdownPdf
from markdown_pdf import Section

# Create a PDF object
pdf = MarkdownPdf(toc_level=2)

# Read the Markdown file
with open("technical_report.md", "r", encoding="utf-8") as f:
    markdown_text = f.read()

# Add the content to the PDF (images will automatically embed from the report folder)
pdf.add_section(Section(markdown_text, root="."))

# Set PDF document properties
pdf.meta["title"] = "E-commerce Analytics Technical Report"
pdf.meta["author"] = "Babatunde Oyedele"

# Save as PDF
pdf.save("technical_report.pdf")
print("PDF generated successfully: technical_report.pdf")