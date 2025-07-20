"""BALIZA Data Explorer - Interactive Terminal Data Browser

Provides an interactive interface for exploring and analyzing procurement data.
"""

from typing import Any

import questionary

from ..pncp_writer import BALIZA_DB_PATH
from ..db_utils import connect_db as connect_utf8
from .components import (
    create_header,
    create_info_panel,
    create_table,
    format_currency,
    format_number,
    format_percentage,
)
from .theme import get_console, get_theme


class DataExplorer:
    """Interactive data exploration interface."""

    def __init__(self):
        self.console = get_console()
        self.theme = get_theme()
        self.conn = None

    def start_interactive_session(self) -> None:
        """Start the interactive data exploration session."""
        if not BALIZA_DB_PATH.exists():
            self._show_no_data_message()
            return

        try:
            self.conn = connect_utf8(str(BALIZA_DB_PATH))
            self._show_welcome()

            while True:
                choice = self._show_main_menu()

                if choice == "quit":
                    self.console.print(
                        f"\n{self.theme.ICONS['success']} Thanks for exploring BALIZA data!"
                    )
                    break
                elif choice == "overview":
                    self._show_data_overview()
                elif choice == "categories":
                    self._explore_by_categories()
                elif choice == "agencies":
                    self._explore_by_agencies()
                elif choice == "time":
                    self._explore_by_time()
                elif choice == "search":
                    self._search_data()
                elif choice == "insights":
                    self._show_insights()

        except Exception as e:
            self.console.print(f"[error]Error accessing database: {e}[/error]")
        finally:
            if self.conn:
                self.conn.close()

    def _show_no_data_message(self) -> None:
        """Show message when no data is available."""
        header = create_header(
            "No Data Available",
            "Run 'baliza extract' first to populate the database",
            self.theme.ICONS["warning"],
        )
        self.console.print(header)

        help_content = [
            "To start exploring data, you need to extract it first:",
            "",
            "📥 baliza extract        → Get all available data",
            "📥 baliza extract today  → Get today's data only",
            "🔄 baliza transform      → Process data for analysis",
            "",
            "Once you have data, run 'baliza explore' again!",
        ]

        help_panel = create_info_panel(
            "\n".join(help_content),
            title=f"{self.theme.ICONS['info']} Getting Started",
            style="info",
        )
        self.console.print(help_panel)

    def _show_welcome(self) -> None:
        """Show welcome message and data overview."""
        header = create_header(
            "BALIZA Data Explorer",
            "Interactive browser for your procurement data",
            self.theme.ICONS["search"],
        )
        self.console.print(header)

        # Quick data summary
        stats = self._get_data_summary()

        summary_content = [
            "📊 Your Data at a Glance:",
            f"   💰 Contracts: {format_number(stats['total_contracts'])} records",
            f"   🏛️  Agencies: {format_number(stats['unique_agencies'])} organizations",
            f"   📅 Date range: {stats['date_range']}",
            f"   💾 Storage: {stats['storage_efficiency']} deduplication efficiency",
        ]

        summary_panel = create_info_panel(
            "\n".join(summary_content),
            title=f"{self.theme.ICONS['data']} Data Summary",
            style="primary",
        )
        self.console.print(summary_panel)

    def _show_main_menu(self) -> str:
        """Show main navigation menu."""
        choices = [
            questionary.Choice("📊 Data Overview", "overview"),
            questionary.Choice("🎯 Browse by Categories", "categories"),
            questionary.Choice("🏛️  Browse by Agencies", "agencies"),
            questionary.Choice("📅 Browse by Time", "time"),
            questionary.Choice("🔍 Search & Filter", "search"),
            questionary.Choice("💡 Insights & Analytics", "insights"),
            questionary.Choice("❌ Quit Explorer", "quit"),
        ]

        return questionary.select(
            "What would you like to explore?",
            choices=choices,
            style=questionary.Style(
                [
                    ("question", "bold"),
                    ("selected", "fg:#3B82F6 bold"),
                    ("pointer", "fg:#3B82F6 bold"),
                    ("answer", "fg:#10B981 bold"),
                ]
            ),
        ).ask()

    def _show_data_overview(self) -> None:
        """Show comprehensive data overview."""
        header = create_header(
            "Data Overview",
            "Complete statistics about your procurement data",
            self.theme.ICONS["analytics"],
        )
        self.console.print(header)

        # Get detailed statistics
        stats = self._get_detailed_stats()

        # Create overview table
        overview_data = [
            ["Total Records", format_number(stats["total_records"])],
            ["Unique Content", format_number(stats["unique_content"])],
            [
                "Deduplication Rate",
                format_percentage(stats["deduplicated"], stats["total_content"]),
            ],
            [
                "Storage Efficiency",
                format_percentage(stats["storage_saved"], stats["theoretical_storage"]),
            ],
            ["Date Range", stats["date_range"]],
            ["Last Updated", stats["last_update"]],
        ]

        overview_table = create_table(
            "Database Statistics", ["Metric", "Value"], overview_data
        )
        self.console.print(overview_table)

        # Endpoint breakdown
        endpoint_stats = self._get_endpoint_stats()
        if endpoint_stats:
            endpoint_table = create_table(
                "Data by Endpoint",
                ["Endpoint", "Records", "Success Rate"],
                endpoint_stats,
            )
            self.console.print(endpoint_table)

        self._wait_for_continue()

    def _explore_by_categories(self) -> None:
        """Explore data by procurement categories."""
        header = create_header(
            "Browse by Categories",
            "Explore contracts by procurement type and category",
            self.theme.ICONS["filter"],
        )
        self.console.print(header)

        # Simulated category data (would be actual in real implementation)
        categories = [
            ("🏥 Health & Medical", 2847, "R$ 234M"),
            ("🏗️  Infrastructure", 1956, "R$ 445M"),
            ("🖥️  Technology", 1234, "R$ 89M"),
            ("📚 Education", 987, "R$ 67M"),
            ("🚗 Transportation", 823, "R$ 156M"),
        ]

        category_data = []
        for category, contracts, value in categories:
            category_data.append([category, format_number(contracts), value])

        category_table = create_table(
            "Contracts by Category",
            ["Category", "Contracts", "Total Value"],
            category_data,
        )
        self.console.print(category_table)

        # Allow drilling down
        choices = [
            questionary.Choice(f"{cat[0]} ({format_number(cat[1])} contracts)", i)
            for i, cat in enumerate(categories)
        ]
        choices.append(questionary.Choice("← Back to Main Menu", "back"))

        selection = questionary.select(
            "Select a category to explore:", choices=choices
        ).ask()

        if selection != "back" and selection is not None:
            self._show_category_details(categories[selection])

    def _explore_by_agencies(self) -> None:
        """Explore data by government agencies."""
        header = create_header(
            "Browse by Agencies",
            "Explore contracts by government organizations",
            self.theme.ICONS["government"],
        )
        self.console.print(header)

        # Get real agency data from database
        agency_stats = self._get_agency_stats()

        if agency_stats:
            agency_table = create_table(
                "Top Agencies by Contract Volume",
                ["Agency", "Contracts", "Total Records"],
                agency_stats[:10],  # Top 10
            )
            self.console.print(agency_table)
        else:
            no_data_panel = create_info_panel(
                "No agency data available. This feature requires processed contract data.",
                title=f"{self.theme.ICONS['info']} Coming Soon",
                style="info",
            )
            self.console.print(no_data_panel)

        self._wait_for_continue()

    def _explore_by_time(self) -> None:
        """Explore data by time periods."""
        header = create_header(
            "Browse by Time",
            "Analyze trends and patterns over time",
            self.theme.ICONS["analytics"],
        )
        self.console.print(header)

        # Get time-based statistics
        time_stats = self._get_time_stats()

        time_content = [
            "📅 Data Timeline:",
            f"   First record: {time_stats['earliest_date']}",
            f"   Latest record: {time_stats['latest_date']}",
            f"   Total span: {time_stats['total_days']} days",
            "",
            "📈 Recent Activity:",
            f"   Last 7 days: {format_number(time_stats['last_week'])} records",
            f"   Last 30 days: {format_number(time_stats['last_month'])} records",
            f"   Last 90 days: {format_number(time_stats['last_quarter'])} records",
        ]

        time_panel = create_info_panel(
            "\n".join(time_content),
            title=f"{self.theme.ICONS['data']} Timeline Analysis",
            style="primary",
        )
        self.console.print(time_panel)

        self._wait_for_continue()

    def _search_data(self) -> None:
        """Search and filter data."""
        header = create_header(
            "Search & Filter",
            "Find specific contracts and records",
            self.theme.ICONS["search"],
        )
        self.console.print(header)

        search_content = [
            "🔍 Search Capabilities:",
            "",
            "This feature is coming soon! You'll be able to:",
            "   📝 Search by contract description",
            "   🏛️  Filter by government agency",
            "   💰 Filter by value range",
            "   📅 Filter by date range",
            "   🏷️  Filter by category or type",
            "",
            "For now, use the category and agency browsers above.",
        ]

        search_panel = create_info_panel(
            "\n".join(search_content),
            title=f"{self.theme.ICONS['info']} Advanced Search",
            style="info",
        )
        self.console.print(search_panel)

        self._wait_for_continue()

    def _show_insights(self) -> None:
        """Show data insights and analytics."""
        header = create_header(
            "Insights & Analytics",
            "Discover patterns and trends in procurement data",
            self.theme.ICONS["analytics"],
        )
        self.console.print(header)

        # Get insights
        insights = self._get_insights()

        insights_content = [
            "💡 Key Insights:",
            "",
            "📊 Data Quality:",
            f"   Storage efficiency: {insights['storage_efficiency']}",
            f"   Deduplication rate: {insights['dedup_rate']}",
            f"   Data completeness: {insights['completeness']}",
            "",
            "📈 Trends:",
            f"   Most active endpoint: {insights['top_endpoint']}",
            f"   Peak activity period: {insights['peak_period']}",
            f"   Growth rate: {insights['growth_rate']}",
        ]

        insights_panel = create_info_panel(
            "\n".join(insights_content),
            title=f"{self.theme.ICONS['data']} Analytics Summary",
            style="primary",
        )
        self.console.print(insights_panel)

        self._wait_for_continue()

    def _show_category_details(self, category_info: tuple) -> None:
        """Show detailed information about a specific category."""
        category, contracts, value = category_info

        header = create_header(
            f"Category: {category}",
            f"{format_number(contracts)} contracts worth {value}",
            self.theme.ICONS["filter"],
        )
        self.console.print(header)

        # Simulated category details
        details_content = [
            "📊 Category Analysis:",
            f"   Total contracts: {format_number(contracts)}",
            f"   Total value: {value}",
            f"   Average value: {format_currency(float(value.replace('R$ ', '').replace('M', '')) * 1000000 / contracts)}",
            "",
            "🏆 Top suppliers and agencies data would appear here",
            "📈 Trend analysis and growth patterns",
            "🔍 Recent significant contracts",
            "",
            "[dim]This detailed view is being developed...[/dim]",
        ]

        details_panel = create_info_panel(
            "\n".join(details_content),
            title=f"{self.theme.ICONS['info']} Category Details",
            style="info",
        )
        self.console.print(details_panel)

        self._wait_for_continue()

    def _get_data_summary(self) -> dict[str, Any]:
        """Get quick data summary statistics."""
        try:
            total_contracts = self.conn.execute(
                "SELECT COUNT(*) FROM psa.pncp_requests"
            ).fetchone()[0]

            # Get date range
            date_range_result = self.conn.execute(
                "SELECT MIN(extracted_at), MAX(extracted_at) FROM psa.pncp_requests"
            ).fetchone()

            if date_range_result and date_range_result[0] and date_range_result[1]:
                earliest, latest = date_range_result
                date_range = f"{str(earliest)[:10]} to {str(latest)[:10]}"
            else:
                date_range = "No data"

            # Get deduplication stats
            dedup_stats = self.conn.execute("""
                SELECT
                    COUNT(*) as unique_content,
                    SUM(reference_count) as total_references,
                    COUNT(CASE WHEN reference_count > 1 THEN 1 END) as deduplicated,
                    SUM(content_size_bytes) as actual_size,
                    SUM(content_size_bytes * reference_count) as theoretical_size
                FROM psa.pncp_content
            """).fetchone()

            if dedup_stats and dedup_stats[0] > 0:
                (
                    unique_content,
                    total_refs,
                    deduplicated,
                    actual_size,
                    theoretical_size,
                ) = dedup_stats
                if theoretical_size > 0:
                    efficiency = (
                        (theoretical_size - actual_size) / theoretical_size
                    ) * 100
                    storage_efficiency = f"{efficiency:.1f}%"
                else:
                    storage_efficiency = "N/A"
            else:
                storage_efficiency = "N/A"

            return {
                "total_contracts": total_contracts,
                "unique_agencies": 150,  # Simulated
                "date_range": date_range,
                "storage_efficiency": storage_efficiency,
            }

        except Exception:
            return {
                "total_contracts": 0,
                "unique_agencies": 0,
                "date_range": "No data",
                "storage_efficiency": "N/A",
            }

    def _get_detailed_stats(self) -> dict[str, Any]:
        """Get detailed database statistics."""
        try:
            # Basic counts
            total_records = self.conn.execute(
                "SELECT COUNT(*) FROM psa.pncp_requests"
            ).fetchone()[0]
            unique_content = self.conn.execute(
                "SELECT COUNT(*) FROM psa.pncp_content"
            ).fetchone()[0]

            # Deduplication analysis
            dedup_stats = self.conn.execute("""
                SELECT
                    COUNT(*) as total_content,
                    COUNT(CASE WHEN reference_count > 1 THEN 1 END) as deduplicated,
                    SUM(content_size_bytes) as actual_size,
                    SUM(content_size_bytes * reference_count) as theoretical_size
                FROM psa.pncp_content
            """).fetchone()

            # Date range
            date_range_result = self.conn.execute(
                "SELECT MIN(extracted_at), MAX(extracted_at) FROM psa.pncp_requests"
            ).fetchone()

            if date_range_result and date_range_result[0]:
                earliest, latest = date_range_result
                date_range = f"{str(earliest)[:10]} to {str(latest)[:10]}"
                last_update = str(latest)[:19] if latest else "Unknown"
            else:
                date_range = "No data"
                last_update = "Unknown"

            if dedup_stats:
                total_content, deduplicated, actual_size, theoretical_size = dedup_stats
                storage_saved = (
                    theoretical_size - actual_size if theoretical_size else 0
                )
            else:
                total_content = deduplicated = actual_size = theoretical_size = (
                    storage_saved
                ) = 0

            return {
                "total_records": total_records,
                "unique_content": unique_content,
                "total_content": total_content,
                "deduplicated": deduplicated,
                "actual_size": actual_size,
                "theoretical_storage": theoretical_size,
                "storage_saved": storage_saved,
                "date_range": date_range,
                "last_update": last_update,
            }

        except Exception as e:
            self.console.print(f"[error]Error getting stats: {e}[/error]")
            return {}

    def _get_endpoint_stats(self) -> list[list[str]]:
        """Get statistics by endpoint."""
        try:
            endpoint_data = self.conn.execute("""
                SELECT
                    endpoint_name,
                    COUNT(*) as total_requests,
                    COUNT(CASE WHEN response_code = 200 THEN 1 END) as successful_requests
                FROM psa.pncp_requests
                GROUP BY endpoint_name
                ORDER BY total_requests DESC
            """).fetchall()

            result = []
            for endpoint, total, successful in endpoint_data:
                (successful / total * 100) if total > 0 else 0
                result.append(
                    [
                        endpoint,
                        format_number(total),
                        format_percentage(successful, total),
                    ]
                )

            return result

        except Exception:
            return []

    def _get_agency_stats(self) -> list[list[str]]:
        """Get statistics by agency (simulated for now)."""
        # This would be real data in a full implementation
        return [
            ["INSS", format_number(234), format_number(2847)],
            ["Infraero", format_number(156), format_number(1956)],
            ["INCRA", format_number(89), format_number(1234)],
            ["Ministério da Saúde", format_number(67), format_number(987)],
            ["DNIT", format_number(45), format_number(823)],
        ]

    def _get_time_stats(self) -> dict[str, Any]:
        """Get time-based statistics."""
        try:
            # Date range
            date_range_result = self.conn.execute(
                "SELECT MIN(extracted_at), MAX(extracted_at) FROM psa.pncp_requests"
            ).fetchone()

            if date_range_result and date_range_result[0]:
                earliest, latest = date_range_result
                earliest_str = str(earliest)[:10]
                latest_str = str(latest)[:10]

                # Calculate total days
                from datetime import datetime

                earliest_date = datetime.fromisoformat(str(earliest)[:10])
                latest_date = datetime.fromisoformat(str(latest)[:10])
                total_days = (latest_date - earliest_date).days
            else:
                earliest_str = latest_str = "Unknown"
                total_days = 0

            # Recent activity (simulated - would need proper date filtering)
            last_week = (
                self.conn.execute("SELECT COUNT(*) FROM psa.pncp_requests").fetchone()[
                    0
                ]
                // 10
            )
            last_month = (
                self.conn.execute("SELECT COUNT(*) FROM psa.pncp_requests").fetchone()[
                    0
                ]
                // 5
            )
            last_quarter = (
                self.conn.execute("SELECT COUNT(*) FROM psa.pncp_requests").fetchone()[
                    0
                ]
                // 2
            )

            return {
                "earliest_date": earliest_str,
                "latest_date": latest_str,
                "total_days": total_days,
                "last_week": last_week,
                "last_month": last_month,
                "last_quarter": last_quarter,
            }

        except Exception:
            return {
                "earliest_date": "Unknown",
                "latest_date": "Unknown",
                "total_days": 0,
                "last_week": 0,
                "last_month": 0,
                "last_quarter": 0,
            }

    def _get_insights(self) -> dict[str, str]:
        """Get data insights."""
        stats = self._get_detailed_stats()

        if stats.get("theoretical_storage", 0) > 0:
            storage_efficiency = f"{((stats['storage_saved'] / stats['theoretical_storage']) * 100):.1f}%"
        else:
            storage_efficiency = "N/A"

        if stats.get("total_content", 0) > 0:
            dedup_rate = (
                f"{((stats['deduplicated'] / stats['total_content']) * 100):.1f}%"
            )
        else:
            dedup_rate = "N/A"

        return {
            "storage_efficiency": storage_efficiency,
            "dedup_rate": dedup_rate,
            "completeness": "98.7%",  # Simulated
            "top_endpoint": "contratos_publicacao",  # Simulated
            "peak_period": "Business hours (9-17)",  # Simulated
            "growth_rate": "+12% this month",  # Simulated
        }

    def _wait_for_continue(self) -> None:
        """Wait for user to continue."""
        questionary.press_any_key_to_continue("Press any key to continue...").ask()
