"""Stage 2: global identity hypotheses and calibrated world track coordinates.

Revision ID: 0002_stage2_global_identity
Revises: 0001_initial_core
"""
from alembic import op
import sqlalchemy as sa

revision = "0002_stage2_global_identity"
down_revision = "0001_initial_core"
branch_labels = None
depends_on = None


def upgrade():
    op.add_column("cameras", sa.Column("calibration", sa.JSON(), nullable=True))
    op.add_column("cameras", sa.Column("floorplan_x", sa.Float(), nullable=True))
    op.add_column("cameras", sa.Column("floorplan_y", sa.Float(), nullable=True))
    op.add_column("track_points", sa.Column("world_x_m", sa.Float(), nullable=True))
    op.add_column("track_points", sa.Column("world_y_m", sa.Float(), nullable=True))
    op.add_column("track_points", sa.Column("world_z_m", sa.Float(), nullable=True))
    op.create_table(
        "global_tracks",
        sa.Column("id", sa.String(length=100), primary_key=True),
        sa.Column("site_id", sa.String(length=36), sa.ForeignKey("sites.id", ondelete="CASCADE"), nullable=False),
        sa.Column("object_class", sa.String(length=64), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("last_seen", sa.DateTime(timezone=True), nullable=False),
    )
    op.create_index("ix_global_tracks_site_id", "global_tracks", ["site_id"])
    op.create_table(
        "global_track_memberships",
        sa.Column("id", sa.String(length=36), primary_key=True),
        sa.Column("global_track_id", sa.String(length=100), sa.ForeignKey("global_tracks.id", ondelete="CASCADE"), nullable=False),
        sa.Column("local_track_id", sa.String(length=100), sa.ForeignKey("tracked_objects.id", ondelete="CASCADE"), nullable=False),
        sa.Column("camera_id", sa.String(length=36), sa.ForeignKey("cameras.id", ondelete="CASCADE"), nullable=False),
        sa.Column("matched_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("confidence", sa.Float(), nullable=False),
        sa.Column("method", sa.String(length=80), nullable=False),
        sa.Column("provenance", sa.String(length=80), nullable=False),
        sa.UniqueConstraint("local_track_id", name="uq_global_membership_local_track"),
    )
    op.create_index("ix_global_track_memberships_global_track_id", "global_track_memberships", ["global_track_id"])
    op.create_index("ix_global_track_memberships_local_track_id", "global_track_memberships", ["local_track_id"])
    op.create_index("ix_global_track_memberships_camera_id", "global_track_memberships", ["camera_id"])


def downgrade():
    op.drop_table("global_track_memberships")
    op.drop_table("global_tracks")
    op.drop_column("track_points", "world_z_m")
    op.drop_column("track_points", "world_y_m")
    op.drop_column("track_points", "world_x_m")
    op.drop_column("cameras", "calibration")
    op.drop_column("cameras", "floorplan_y")
    op.drop_column("cameras", "floorplan_x")
